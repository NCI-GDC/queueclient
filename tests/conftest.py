import logging
import os
from threading import Thread

import pytest
from depot import Depot
from werkzeug.serving import make_server

from queueclient import QueueFactory

logging.basicConfig(level=logging.ERROR)


class DepotServer(Thread):
    """
    A basic Depot flask server that runs in a separate
    thread.
    """

    def __init__(self):
        Thread.__init__(self)
        self.depot = Depot()
        self.depot.app.testing = True
        self.client = self.depot.app.test_client()
        self.server = make_server("localhost", 5000, self.depot.app)

    def run(self):
        self.server.serve_forever()

    def stop(self):
        """
        Shuts down the running server instance
        """
        self.server.shutdown()


@pytest.fixture(scope="session")
def depot_fixture(request):

    mock = DepotServer()
    mock.start()

    def tear_down():
        # stop depot server
        mock.stop()
    request.addfinalizer(tear_down)
    return mock.server.host, mock.server.port


@pytest.fixture()
def rmq_fixture():
    rbmq_host = os.environ.get("RABBITMQ_SERVER", "localhost")
    rbmq_vhost = os.environ.get("RABBITMQ_VHOST", "/")
    rbmq_qid = os.environ.get("RABBITMQ_QUEUE", "xtest")
    q1 = QueueFactory.get_rabbitmq_client(queue_id=rbmq_qid, host=rbmq_host, vhost=rbmq_vhost, durable=False)
    q2 = QueueFactory.get_rabbitmq_client(queue_id=rbmq_qid, host=rbmq_host, vhost=rbmq_vhost, durable=False)
    yield q1, q2

    q1.close()
    q2.close()
