import os

from deprecated import deprecated

from queueclient.core import InMemoryQueueClient, QueueClient
from queueclient.depot import DepotQueueClient
from queueclient.rabbitmq import RabbitMQClient


class QueueFactory:

    active_in_memory_queues = {}

    @staticmethod
    def get_in_memory_client(queue_id: str) -> QueueClient:
        if queue_id not in QueueFactory.active_in_memory_queues:
            QueueFactory.active_in_memory_queues[queue_id] = InMemoryQueueClient(
                queue_id=queue_id
            )
        return QueueFactory.active_in_memory_queues[queue_id]

    @staticmethod
    def get_rabbitmq_client(
        queue_id,
        host="rabbitmq.service.consul",
        vhost="/dev",
        port=5672,
        username="guest",
        password="guest",
        durable=True,
    ):

        rabbitmq_url = os.environ.get("RABBITMQ_SERVER", host)
        rabbitmq_port = int(os.environ.get("RABBITMQ_PORT", port))
        rabbitmq_vhost = os.environ.get("RABBITMQ_VHOST", vhost)
        rabbitmq_user = os.environ.get("RABBITMQ_USER", username)
        rabbitmq_pwd = os.environ.get("RABBITMQ_PWD", password)

        return RabbitMQClient(
            host=rabbitmq_url,
            port=rabbitmq_port,
            vhost=rabbitmq_vhost,
            queue_id=queue_id,
            username=rabbitmq_user,
            password=rabbitmq_pwd,
            durable=durable,
        )

    @staticmethod
    @deprecated(reason="Depot server is no longer maintained.")
    def get_depot_client(queue_id, host="depot.service.consul", port=80, version="v0"):
        depot_server = os.environ.get("DEPOT_SERVER", host)
        depot_port = int(os.environ.get("DEPOT_PORT", port))
        return DepotQueueClient(
            host=depot_server, queue_id=queue_id, port=depot_port, version=version
        )
