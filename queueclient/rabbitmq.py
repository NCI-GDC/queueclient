import functools
import json
import threading

import pika
from pika.exceptions import ConnectionClosed, ChannelClosed

from queueclient.core import QueueClient


class RabbitMQClient(QueueClient):

    def __init__(self, host="rabbitmq.service.consul", vhost="/dev",
                 port=5672, queue_id="dev", username="guest", password="guest", durable=True):
        """
        Args:
            host (str): RabbitMQ server host or IP
            vhost (str): Virtaul host defined on RabbitMQ
            port (int): defaults to 5672, the default rabbitmq port
            queue_id (str): queue name to bind to, created if it does not exist
            username (str): username
            password (str): password
            durable (bool): durable queues survive server restarts
        """
        self.host = host
        self.port = port
        self.vhost = vhost
        self.durable = durable
        self.queue_id = queue_id

        self.channel = None
        self.connection = None

        creds = pika.PlainCredentials(username, password, erase_on_connect=True)
        self.params = pika.ConnectionParameters(host=host, heartbeat=600, blocked_connection_timeout=300,
                                                port=port, virtual_host=vhost, credentials=creds)

        super(RabbitMQClient, self).__init__(queue_id=queue_id)

    def _get_channel(self):

        if self.ping() is False:
            self.setup()
        return self.channel

    def setup(self):

        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(self.queue_id, durable=self.durable)

    def enqueue(self, msg, durable=True, exchange=""):

        # to json
        msg = json.dumps(msg)
        channel = self._get_channel()
        delivery_mode = 2 if durable else 1  # mode 2 == durable, 1 == not durable
        props = pika.BasicProperties(delivery_mode=delivery_mode)

        # TODO use of non default exchange
        return channel.basic_publish(exchange, routing_key=self.queue_id, body=msg, properties=props)

    def consume(self, callback, requeue_failed=True, on_failure_callback=None):
        """ Listens for incoming data in queue
            Args:
                callback: function in the form
                    def callback(body):
                        Args:`
                            body: response retrieved from queue`
                        do something
                requeue_failed (bool): If True requeue task on failure
                on_failure_callback (function): external handling of failed tasks, same signature as callback
        """
        channel = self._get_channel()

        channel.basic_qos(prefetch_count=1)

        # async handling for consuming requests so it does not get dropped via heartbeat mechanism
        # Note: basic_consume has a method signature requirement for its callback function
        # this partial is to ensure this requirement is met
        on_basic_consume = functools.partial(self._basic_callback,
                                             callback=callback,
                                             requeue_failed=requeue_failed,
                                             on_failure=on_failure_callback)

        channel.basic_consume(on_basic_consume, queue=self.queue_id)
        channel.start_consuming()

    def dequeue(self):
        """ Opens connection, performs consume and closes connection, returns response.  """
        channel = self._get_channel()
        mtd, props, body = channel.basic_get(self.queue_id)

        if body:
            try:
                # py3 returns bytes
                if isinstance(body, bytes):
                    body = body.decode("utf-8")
                body = json.loads(body)

                # acknowledge receipt
                channel.basic_ack(delivery_tag=mtd.delivery_tag)
            except Exception as e:
                # requeue failure
                channel.basic_nack(delivery_tag=mtd.delivery_tag, requeue=True)
                raise e
        return body

    def status(self):
        channel = self._get_channel()
        return channel.is_open()

    def ping(self):
        """ Not required """
        if self.channel is None:
            return False

        # try to use the channel
        try:
            self.channel.exchange_declare('dummy', passive=True)
            return True
        except ConnectionClosed:
            return False
        except ChannelClosed:
            return False

    def close(self):
        if self.channel:
            self.channel.stop_consuming()
            self.channel.close()
        if self.connection:
            self.connection.close()

    def _basic_callback(self, channel, method, props, body, callback, requeue_failed=True, on_failure=None):
        """ Wraps user provided callback function in a thread """
        delivery_tag = method.delivery_tag
        try:
            t = threading.Thread(target=self._handle_callback,
                                 args=(channel, delivery_tag, body, callback, requeue_failed, on_failure))
            t.start()
            t.join()
        except Exception:
            self.logger.error("Exception while processing request", exc_info=1)

    def _handle_callback(self, channel, delivery_tag, body, callback, requeue_failed=True, on_failure=None):
        """ Wraps user provided callback and adds basic acknowledgement when nothing goes wrong"""
        def ack_message():
            if channel.is_open:
                channel.basic_ack(delivery_tag)
            else:
                self.logger("Channel is already closed, message cannot be acknowledged")

        def nack_message():
            if channel.is_open:
                channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue_failed)
            on_failure(body)

        try:
            # py3 returns bytes
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            callback(body)

            if self.connection.is_open:
                self.connection.add_callback_threadsafe(ack_message)

        except Exception:
            if self.connection.is_open:
                self.connection.add_callback_threadsafe(nack_message)

            self.logger.error("Exception while processing request", exc_info=1)
