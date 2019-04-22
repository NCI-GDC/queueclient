import json
import os
import sched
import threading

import time

import uuid

from queueclient import InMemoryQueueClient, DepotQueueClient, QueueFactory


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


def test_listening_inmemory_queue():
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


def test_depot_queue(depot_fixture):
    """Tests reading and writing to supported queue types"""
    host, port = depot_fixture
    q = DepotQueueClient(host=host, port=port, queue_id=str(uuid.uuid4()))
    assert q.status() is True

    response = q.enqueue(msg=dict(did="AAAAA", size=123))

    assert response is True

    # retrieve
    msg = q.dequeue()
    assert msg
    assert msg["did"] == "AAAAA"
    assert msg["size"] == 123


def test_listening_depot_queue(depot_fixture):
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
        q.close()

    q.consume(callback=consume)


def test_rabbitmq_queue():
    """Tests reading and writing to supported queue types"""

    rbmq_host = os.environ.get("RABBITMQ_SERVER", "localhost")
    rbmq_vhost = os.environ.get("RABBITMQ_VHOST", "/")
    rbmq_qid = os.environ.get("RABBITMQ_QUEUE", "xtest")
    q = QueueFactory.get_rabbitmq_client(queue_id=rbmq_qid, host=rbmq_host, vhost=rbmq_vhost, durable=False)

    def consumer_callback(body):
        msg = json.loads(body)

        assert msg
        assert msg["did"] == "AAAAA"
        assert msg["size"] == 123
        q.close()

    response = q.enqueue(msg=dict(did="AAAAA", size=123))
    assert response is True
    q.consume(consumer_callback, requeue_failed=False)


def test_on_failure_callback():
    rbmq_host = os.environ.get("RABBITMQ_SERVER", "localhost")
    rbmq_vhost = os.environ.get("RABBITMQ_VHOST", "/")
    rbmq_qid = os.environ.get("RABBITMQ_QUEUE", "xtest")
    q = QueueFactory.get_rabbitmq_client(queue_id=rbmq_qid, host=rbmq_host, vhost=rbmq_vhost, durable=False)

    def consumer_callback(body):
        raise ValueError("misunderstood teens")

    def f_call(body):
        msg = json.loads(body)

        assert msg
        assert msg["did"] == "AAAAA"
        assert msg["size"] == 123
        q.close()

    response = q.enqueue(msg=dict(did="AAAAA", size=123))
    assert response is True
    q.consume(consumer_callback, requeue_failed=False, on_failure_callback=f_call)
