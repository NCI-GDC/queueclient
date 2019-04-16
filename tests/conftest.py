import logging
from threading import Thread

import pytest
from depot import Depot
from werkzeug.serving import make_server

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
        # stop Moto server
        mock.stop()
    request.addfinalizer(tear_down)
    return mock.server.host, mock.server.port
