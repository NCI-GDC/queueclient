import pika
from pika.exceptions import ConnectionClosed

from gdcqc.core import ServiceQueue


class RabbitMQServiceQueue(ServiceQueue):

    def __init__(self, host="rabbitmq.service.consul", vhost="/dev",
                 port=5672, queue_id="dev", username="guest", password="guest", durable=True):

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

        super(RabbitMQServiceQueue, self).__init__(queue_id=queue_id)

    def _get_channel(self):

        if self.ping() is False:
            self.setup()
        return self.channel

    def setup(self):

        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(self.queue_id, durable=self.durable)

    def enqueue(self, msg, durable=True):

        channel = self._get_channel()
        delivery_mode = 2 if durable else 1
        props = pika.BasicProperties(delivery_mode=delivery_mode)
        channel.basic_publish('', routing_key=self.queue_id, body=msg, properties=props)
        return True

    def consume(self, callback):
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
            self.logger.warning("Connection Closed, retry connecting")
        return False

    def close(self):

        if self.channel:
            self.channel.close()
        if self.connection:
            self.connection.close()


if __name__ == '__main__':
    qs = RabbitMQServiceQueue("172.21.23.222", username="dev_gdc", password="s3cr3t", queue_id="test")
    print(qs.dequeue())
