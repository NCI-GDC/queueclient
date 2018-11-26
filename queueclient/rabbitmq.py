import json

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

    def enqueue(self, msg, durable=True):
        """ Adds an item to the queue `queue-id` using default exchange
        Args:
            msg (object): JSON serializable object
            durable (bool): if True, the entry survives server restart
        Returns:
            bool: True
        """
        msg = json.dumps(msg)
        channel = self._get_channel()
        delivery_mode = 2 if durable else 1  # mode 2 == durable, 1 == not durable
        props = pika.BasicProperties(delivery_mode=delivery_mode)

        # TODO use of non default exchange
        return channel.basic_publish('', routing_key=self.queue_id, body=msg, properties=props)

    def consume(self, callback):
        """ Listens for incoming data in queue
            Args:
                callback: function in the form
                    def callback(ch, mtd, props, body):
                        Args:
                            ch: current channel
                            mtd:
                            props:`
                            body: response retrieved from queue`
                        do something
        """
        channel = self._get_channel()

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(callback, queue=self.queue_id)
        channel.start_consuming()

    def dequeue(self):
        """ Opens connection, performs consume and closes connection, returns response.  """
        channel = self._get_channel()
        mtd, props, body = channel.basic_get(self.queue_id)

        if body:
            # acknowledge receipt if something was received
            channel.basic_ack(delivery_tag=mtd.delivery_tag)
        body = json.loads(body)
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
            self.channel.close()
        if self.connection:
            self.connection.close()
