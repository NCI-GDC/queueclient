import json
import uuid
from threading import Thread
from typing import Tuple, cast

from queueclient import RabbitMQClient
from queueclient.core import InMemoryQueueClient
from queueclient.depot import DepotQueueClient
from queueclient.queue_factory import QueueFactory


def test_inmemory_queue():
    """Tests initializing supported queue types"""
    # get default queue
    q = InMemoryQueueClient(str(uuid.uuid4()))
    assert q.status() is True

    response = q.enqueue(msg=dict(did="AAAAA", size=123))
    assert response is True

    # retrieve
    msg = q.dequeue()
    assert msg
    assert msg["did"] == "AAAAA"
    assert msg["size"] == 123


def test_inmemory_queue_same_uuid_return_same_queue():
    """Tests initializing supported queue types"""
    # get default queue
    q_uuid = str(uuid.uuid4())
    q = QueueFactory.get_in_memory_client(q_uuid)
    assert q.status() is True

    response = q.enqueue(msg=dict(did="AAAAA", size=123))
    assert response is True

    # retrieve
    q2 = QueueFactory.get_in_memory_client(q_uuid)
    msg = q2.dequeue()
    assert msg
    assert msg["did"] == "AAAAA"
    assert msg["size"] == 123


def test_inmemory_queue_listening():
    # add dummy data to queue
    q = InMemoryQueueClient(str(uuid.uuid4()))
    assert q.status() is True

    response = q.enqueue(msg=dict(did="AAAAA", size=123))
    assert response is True

    def consume(msg):
        assert msg
        assert msg["did"] == "AAAAA"
        assert msg["size"] == 123
        q.close()

    q.consume(callback=consume)


def test_inmemory_queue_completes():
    """When the queue is empty, calling close should exit"""
    q = InMemoryQueueClient(str(uuid.uuid4()))

    def listen_for_messages():
        """Wrapper for the thread"""
        assert q.status() is True

        def consume(msg):
            pass

        q.consume(callback=consume, exit_trigger=lambda: True)

    t = Thread(target=listen_for_messages)
    t.start()
    q.close()
    t.join()


def test_depot_queue(depot_fixture):
    """Tests reading and writing to supported queue types"""
    host, port = depot_fixture
    q = DepotQueueClient(host=host, port=port, queue_id=str(uuid.uuid4()))
    assert q.status() is True

    response = q.enqueue(msg=dict(did="AAAAA", size=123))
    assert response is True

    # retrieve
    msg = cast(dict, q.dequeue())
    assert msg
    assert msg["did"] == "AAAAA"
    assert msg["size"] == 123

    response = q.enqueue(msg=dict(did="BBBBB", size=321))
    assert response is True

    q.clear()
    msg = q.dequeue()
    assert msg is None, "No work should be left to do because queue has been cleared"


def test_depot_queue_listening(depot_fixture):
    # add dummy data to queue
    host, port = depot_fixture
    q = DepotQueueClient(host=host, port=port, queue_id=str(uuid.uuid4()))
    assert q.status() is True

    response = q.enqueue(msg=dict(did="AAAAA", size=123))
    assert response is True

    def consume(msg):
        assert msg
        assert msg["did"] == "AAAAA"
        assert msg["size"] == 123

    q.consume(callback=consume, exit_trigger=lambda: True)


def test_rabbitmq_queue(rabbitmq_clients: Tuple[RabbitMQClient, RabbitMQClient]) -> None:
    """Tests reading and writing to supported queue types"""

    q, qx = rabbitmq_clients

    response = q.enqueue(msg=dict(did="AAAAA", size=123))
    assert response is True

    def consumer_callback(body):
        msg = json.loads(body)

        assert msg
        assert msg["did"] == "AAAAA"
        assert msg["size"] == 123

    qx.consume(consumer_callback, requeue_failed=False, exit_trigger=lambda: True)


def test_rabbitmq_on_failure_callback(
    rabbitmq_clients: Tuple[RabbitMQClient, RabbitMQClient]
) -> None:

    q, qx = rabbitmq_clients

    response = q.enqueue(msg=dict(did="AAAAA", size=123))
    assert response is True

    def consumer_callback(body):
        raise ValueError("misunderstood teens")

    def f_call(body):
        msg = json.loads(body)
        assert msg
        assert msg["did"] == "AAAAA"
        assert msg["size"] == 123
        qx.start_closing()

    qx.consume(
        consumer_callback,
        requeue_failed=False,
        on_failure_callback=f_call,
    )


def test_rabbitmq_deque(rabbitmq_clients: Tuple[RabbitMQClient, RabbitMQClient]) -> None:
    q, qx = rabbitmq_clients

    response = q.enqueue(msg=dict(did="AAAAA", size=123))
    assert response is True

    msg = qx.dequeue()
    assert msg
    assert msg["did"] == "AAAAA"
    assert msg["size"] == 123
