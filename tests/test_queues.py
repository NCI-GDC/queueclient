import json
import os
import uuid

from queueclient import InMemoryQueueClient, DepotQueueClient, RabbitMQClient


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


def test_depot_queue(depot_fixture):
    """Tests reading and writing to supported queue types"""
    q = DepotQueueClient(depot_fixture, queue_id=str(uuid.uuid4()))
    assert q.status() is True

    response = q.enqueue(msg=dict(did="AAAAA", size=123))

    assert response is True

    # retrieve
    msg = q.dequeue()
    assert msg
    assert msg["did"] == "AAAAA"
    assert msg["size"] == 123


def test_rabbitmq_queue():
    """Tests reading and writing to supported queue types"""

    rbmq_host = os.environ.get("RABBITMQ_SERVER", "localhost")
    rbmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rbmq_pwd = os.environ.get("RABBITMQ_PWD", "guest")
    rbmq_vhost = os.environ.get("RABBITMQ_VHOST", "/")
    rbmq_qid = os.environ.get("RABBITMQ_QUEUE", "xtest")
    q = RabbitMQClient(host=rbmq_host, vhost=rbmq_vhost, username=rbmq_user,
                       password=rbmq_pwd, queue_id=rbmq_qid, durable=False)

    def consumer_callback(ch, mtd, props, body):
        msg = json.loads(body)

        assert msg
        assert msg["did"] == "AAAAA"
        assert msg["size"] == 123
        ch.basic_ack(delivery_tag=mtd.delivery_tag)
        ch.cancel()
        q.close()

    response = q.enqueue(msg=dict(did="AAAAA", size=123))
    assert response is True
    q.consume(consumer_callback)


def consumer_callback(ch, mtd, props, body):
    msg = json.loads(body)

    print(msg)
    ch.basic_ack(delivery_tag=mtd.delivery_tag)
