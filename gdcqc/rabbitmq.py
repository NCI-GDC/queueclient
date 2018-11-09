import pika

from gdcqc.core import ServiceQueue


class RabbitMQServiceQueue(ServiceQueue):

    def __init__(self, host="rabbitmq.service.consul", vhost="/dev",
                 port=5672, queue_id="dev", username="guest", password="guest"):

        self.queue_id = queue_id
        self.host = host
        self.port = port
        self.vhost = vhost

        creds = pika.PlainCredentials(username, password, erase_on_connect=True)
        self.params = pika.ConnectionParameters(host=host, port=port, virtual_host=vhost, credentials=creds)
        self.connection = None
        self.channel = None

        super(RabbitMQServiceQueue, self).__init__(queue_id=queue_id)

    def _get_connection(self):

        if self.connection is None or self.connection.is_open is False:
            self.setup()
        return self.channel

    def setup(self):
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(self.queue_id)

    def enqueue(self, msg):
        self.channel.publish('', routing_key=self.queue_id, body=msg)
        return True

    def consume(self, callback):
        channel = self._get_connection()

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(callback, queue=self.queue_id)
        channel.start_consuming()

    def dequeue(self):
        raise NotImplementedError("Feature not supported for broker, use consume")

    def delete(self):
        raise NotImplementedError("Feature not supported for broker, use consume")

    def status(self):
        raise NotImplementedError("Feature not supported for broker, use consume")

    def ping(self):
        self._get_connection()

    def close(self):

        if self.channel:
            self.channel.close()
        if self.connection:
            self.connection.close()


def callback(ch, mtd, props, body):
    print body
    ch.basic_ack(delivery_tag=mtd.delivery_tag)


if __name__ == '__main__':
    qs = RabbitMQServiceQueue("172.21.23.222", username="dev_gdc", password="s3cr3t")
    qs.consume(callback)
