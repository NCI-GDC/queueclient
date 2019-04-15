import json

from queueclient.core import InMemoryQueueClient
from queueclient.depot import DepotQueueClient
from queueclient.rabbitmq import RabbitMQClient


def consumer_callback(body):
    msg = json.loads(body)
    print msg


if __name__ == '__main__':
    qc = RabbitMQClient(host="172.21.23.222", username="dev_gdc", password="s3cr3t", queue_id="test_dev")
    qc.consume(consumer_callback)
