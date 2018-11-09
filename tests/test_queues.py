import json
import uuid

from gdcqc import InMemoryQueue, DepotServiceQueue, RabbitMQServiceQueue


def test_inmemory_queue():
    """Tests initializing supported queue types"""
    # get default queue
    q = InMemoryQueue(str(uuid.uuid4()))
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
    q = DepotServiceQueue(depot_fixture, queue_id=str(uuid.uuid4()))
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

    q = RabbitMQServiceQueue(host="localhost", vhost="/")

    def consumer_callback(ch, mtd, props, body):
        msg = json.loads(body)

        assert msg
        assert msg["did"] == "AAAAA"
        assert msg["size"] == 123
        ch.basic_ack(delivery_tag=mtd.delivery_tag)
        ch.cancel()
        q.close()

    response = q.enqueue(msg=json.dumps(dict(did="AAAAA", size=123)))
    assert response is True
    q.consume(consumer_callback)
