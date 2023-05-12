import logging
import os

from queueclient import RabbitMQClient


def call_back(body):
    print(body)


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

    logging.getLogger("pika").setLevel(logging.ERROR)

    rabbitmq_url = os.getenv("RABBITMQ_SERVER", "rabbitmq.service.consul")
    rabbitmq_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    rabbitmq_vhost = os.getenv("RABBITMQ_VHOST", "/dev")
    rabbitmq_user = os.getenv("RABBITMQ_USER", "guest")
    rabbitmq_pwd = os.getenv("RABBITMQ_PWD", "guest")

    print(rabbitmq_url)
    rmq = RabbitMQClient(
        host=rabbitmq_url,
        port=rabbitmq_port,
        vhost=rabbitmq_vhost,
        queue_id="x_cult_x",
        username=rabbitmq_user,
        password=rabbitmq_pwd,
    )

    rmq.consume(call_back)
