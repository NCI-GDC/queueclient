import abc
import functools
import logging

import pika
import simplejson as json

from queueclient.core import QueueClient


class RabbitMQClient(QueueClient):

    def __init__(self, host="rabbitmq.service.consul", vhost="/dev",
                 port=5672, queue_id="dev", username="guest", password="guest", durable=True,
                 exchange="queueclient", exchange_type="direct", routing_key="queueclient"):
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
        self.username = username
        self.password = password

        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key

        self.client = None
        super(RabbitMQClient, self).__init__(queue_id=queue_id)

    def connect(self):
        try:
            self.client.connect()
        except Exception as e:
            raise ValueError("RabbitMQ connection unsuccessful %s", e)

    def enqueue(self, msg, durable=True, routing_key=None):
        if not isinstance(self.client, RabbitPublisher):
            self.client = RabbitPublisher(self.host, self.vhost, self.port,
                                          self.queue_id, self.username, self.password, self.durable, self.exchange,
                                          self.exchange_type, self.routing_key)
            self.connect()
        routing_key = routing_key or self.routing_key
        self.client.basic_publish(msg, durable, routing_key)
        return True

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

        self.client = RabbitConsumer(self.host, self.vhost, self.port,
                                     self.queue_id, self.username, self.password, self.durable, self.exchange,
                                     self.exchange_type, self.routing_key)
        self.client._callback = callback
        self.client._requeue_failed = requeue_failed
        self.client._on_failure_callback = on_failure_callback

        self.connect()
        self.client.start()

    def dequeue(self):
        """ Opens connection, performs consume and closes connection, returns response.  """

        self.client = RabbitPublisher(self.host, self.vhost, self.port,
                                      self.queue_id, self.username, self.password, self.durable, self.exchange,
                                      self.exchange_type, self.routing_key)

        self.connect()
        body = self.client.basic_get()
        self.close()
        return body

    def status(self):
        return self.client.channel.is_open()

    def ping(self):
        """ Not required """
        if self.client.channel is None:
            return False

        return True

    def close(self):
        self.client.close()


class RabbitConnection(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, host="rabbitmq.service.consul", vhost="/dev",
                 port=5672, queue_id="dev", username="guest", password="guest", durable=True,
                 exchange="queueclient", exchange_type="direct", routing_key="queueclient"):
        """
        Args:
            host (str): RabbitMQ server host or IP
            vhost (str): Virtual host defined on RabbitMQ
            port (int): defaults to 5672, the default rabbitmq port
            queue_id (str): queue name to bind to, created if it does not exist
            username (str): username
            password (str): password
            durable (bool): durable queues survive server restarts
        """

        self.durable = durable
        self.queue_id = queue_id
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key

        self.channel = None
        self.connection = None

        self._is_closing = False
        self.consumer_tag = None

        credentials = pika.PlainCredentials(username, password, erase_on_connect=True)
        self.conn_params = pika.ConnectionParameters(host=host, heartbeat=600, blocked_connection_timeout=300,
                                                     port=port, virtual_host=vhost, credentials=credentials)
        self.logger = logging.getLogger(self.__module__ + "." + self.__class__.__name__)

    @abc.abstractmethod
    def connect(self):
        pass

    def start(self):
        pass


class RabbitConsumer(RabbitConnection):

    def __init__(self, host="rabbitmq.service.consul", vhost="/dev",
                 port=5672, queue_id="dev", username="guest", password="guest", durable=True,
                 exchange="queueclient", exchange_type="direct", routing_key="queueclient"):

        super(RabbitConsumer, self).__init__(host, vhost, port, queue_id, username, password, durable, exchange,
                                             exchange_type,
                                             routing_key)

        self._callback = None
        self._on_failure_callback = None
        self._requeue_failed = True

        self._is_consuming = False

    def connect(self):
        self.connection = pika.SelectConnection(self.conn_params,
                                                on_open_callback=self.on_connection_open,
                                                on_close_callback=self.on_connection_closed,
                                                on_open_error_callback=self.on_connection_open_error)

    def on_connection_closed(self, _conn, reason):
        self.channel = None
        self.logger.error("An exception occured {}, {}".format(_conn, reason))
        if self._is_closing:
            # closing is intentional
            self.connection.ioloop.stop()
        else:
            # closing not intentional, try to schedule restart
            self.stop()

    def on_connection_open_error(self, _unused_connection, err):
        self.logger.error('Connection open failed: %s', err)

    def on_connection_open(self, _conn):
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)

        # setup exchange
        self.channel.exchange_declare(exchange=self.exchange, durable=self.durable,
                                      exchange_type=self.exchange_type, callback=self.on_exchange_declare_ok)

    def on_channel_closed(self, channel, reason):
        self.channel = None
        if self._is_closing and not self.connection.is_closing and not self.connection.is_closed:
            self.connection.close()
        self.logger.error('Channel %i closed: %s', channel, reason)

    def on_exchange_declare_ok(self, _header):
        self.channel.queue_declare(queue=self.queue_id, durable=self.durable, callback=self.on_queue_declare_ok)

    def on_queue_declare_ok(self, _header):
        self.channel.queue_bind(queue=self.queue_id, exchange=self.exchange,
                                routing_key=self.routing_key, callback=self.on_bind_ok)

    def on_bind_ok(self, _header):
        self.channel.basic_qos(prefetch_count=1, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _header):
        # start consuming
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)
        on_basic_consume = functools.partial(self._basic_callback,
                                             callback=self._callback,
                                             requeue_failed=self._requeue_failed,
                                             on_failure=self._on_failure_callback)
        self.consumer_tag = self.channel.basic_consume(queue=self.queue_id, consumer_callback=on_basic_consume)
        self._is_consuming = True

    def on_consumer_cancelled(self, _frame):
        if self.channel:
            self.channel.close()
        self.logger.error('RabbitConsumer channel closed unexpectedly: %s', _frame)

    def close(self):
        self._is_closing = True
        if self.channel is not None:
            self.channel.close()
        if self.connection is not None:
            self.connection.close()

    def _basic_callback(self, channel, method, props, body, callback, requeue_failed=True, on_failure=None):
        """ Wraps user provided callback and adds basic acknowledgement when nothing goes wrong"""
        delivery_tag = method.delivery_tag

        def ack_message():
            if channel.is_open:
                channel.basic_ack(delivery_tag)
            else:
                self.logger.info("Channel is already closed, message cannot be acknowledged")

        def nack_message():
            if channel.is_open:
                channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue_failed)
            if on_failure:
                on_failure(body)

        try:
            # py3 returns bytes
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            callback(body)
            ack_message()
        except Exception:
            nack_message()
            self.logger.error("Exception while processing request", exc_info=1)

    def start(self):
        self.connection.ioloop.start()

    def stop(self):
        if not self._is_closing:
            self._is_closing = True
            self.logger.info('Stopping')
            if self._is_consuming:
                self.channel.basic_cancel(consumer_tag=self.consumer_tag, callback=self.on_cancel_ok)
                self.connection.ioloop.start()
            else:
                self.connection.ioloop.stop()

    def on_cancel_ok(self):
        self._is_consuming = False
        self.channel.close()


class RabbitPublisher(RabbitConnection):

    def connect(self):
        self.connection = pika.BlockingConnection(self.conn_params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type, durable=self.durable)
        self.channel.queue_declare(self.queue_id, durable=self.durable)
        self.channel.queue_bind(queue=self.queue_id, exchange=self.exchange, routing_key=self.routing_key)
        self.logger.debug("Blocking Connection established to {}".format(self.conn_params))

    def basic_publish(self, msg, durability, routing_key):

        msg = json.dumps(msg)
        delivery_mode = 2 if durability else 1  # mode 2 == durable, 1 == not durable
        props = pika.BasicProperties(delivery_mode=delivery_mode, content_type="application/json",
                                     content_encoding="utf-8")
        return self.channel.basic_publish(self.exchange, routing_key=routing_key, body=msg, properties=props)

    def basic_get(self):
        mtd, props, body = self.channel.basic_get(self.queue_id)

        if body:
            try:
                # py3 returns bytes
                if isinstance(body, bytes):
                    body = body.decode("utf-8")
                body = json.loads(body)

                # acknowledge receipt
                self.channel.basic_ack(delivery_tag=mtd.delivery_tag)
            except Exception as e:
                # requeue failure
                self.channel.basic_nack(delivery_tag=mtd.delivery_tag, requeue=True)
                raise e
        return body

    def close(self):
        self._is_closing = True
        self.connection.close()
