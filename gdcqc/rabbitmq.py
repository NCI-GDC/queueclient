import pika

from gdcqc.core import ServiceQueue


class RabbitMQServiceQueue(ServiceQueue):

    def __init__(self, host="rabbitmq.service.consul", vhost="/dev",
                 port=5672, queue_id="dev", username="guest", password="guest", durable=True):

        self.host = host
        self.port = port
        self.vhost = vhost
        self.durable = durable
        self.queue_id = queue_id
        self._dquequed_element = None

        self.channel = None
        self.connection = None

        creds = pika.PlainCredentials(username, password, erase_on_connect=True)
        self.params = pika.ConnectionParameters(host=host, heartbeat=600, blocked_connection_timeout=300,
                                                port=port, virtual_host=vhost, credentials=creds)

        super(RabbitMQServiceQueue, self).__init__(queue_id=queue_id)

    def _get_channel(self):

        if self.connection is None or self.connection.is_open is False:
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
        self._dquequed_element = None

        def _callback(ch, mtd, props, body):
            ch.basic_ack(delivery_tag=mtd.delivery_tag)
            self._dquequed_element = body
            self.close()

        self.consume(_callback)
        return self._dquequed_element

    def status(self):
        raise NotImplementedError("Feature not supported for broker, use consume")

    def ping(self):
        """ Not required """
        return 1

    def close(self):

        if self.channel:
            self.channel.close()
        if self.connection:
            self.connection.close()


if __name__ == '__main__':
    qs = RabbitMQServiceQueue("172.21.23.222", username="dev_gdc", password="s3cr3t", queue_id="test")
    print(qs.dequeue())
