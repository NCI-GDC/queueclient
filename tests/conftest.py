import logging
import os
from datetime import datetime
from threading import Thread
from typing import Tuple

import depot
import pytest
from flask import Flask
from testcontainers.rabbitmq import RabbitMqContainer
from werkzeug.serving import make_server

from queueclient import RabbitMQClient
from queueclient.queue_factory import QueueFactory

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def _make_depot_app():
    app = Flask(__name__)
    # it is important that setup_default_handlers is called before
    # setting up more specific error handlers so that we don't
    # override them
    depot.setup_default_handlers(app)
    app.register_error_handler(500, depot.server_error)
    app.register_blueprint(depot.v0, name="v0", url_prefix="/v0")
    app.register_blueprint(depot.v0, name="v0_latest", url_prefix="/latest")
    app.add_url_rule("/", "root", depot.root)
    return app


class _Depot:
    def __init__(self, settings=None, debug=False):
        """"""
        self.app = _make_depot_app()

        self.settings = settings
        self.app.queue_data = {}
        self.app.start_time = datetime.now()
        self.app.version = "vt"
        logger.info("Initializing depot at {}".format(self.app.start_time))

    def run(self, *args, **kwargs):
        self.app.run(*args, **kwargs)


class DepotServer(Thread):
    """
    A basic Depot flask server that runs in a separate
    thread.
    """

    def __init__(self):
        Thread.__init__(self)
        self.depot = _Depot()
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
def rabbitmq_clients(request: pytest.FixtureRequest) -> Tuple[RabbitMQClient, RabbitMQClient]:
    """Start a RabbitMQ running on port 5672.

    Starts a rabbitmq docker container using testcontainers.
    """
    from pika.connection import Parameters

    # It might take some time to start the image, retrying
    # connections more than once (the default) helps eliminate
    # failing tests due to connection timeout
    Parameters.DEFAULT_CONNECTION_ATTEMPTS = 10

    image_version = os.getenv("RABBITMQ_IMAGE_VERSION", "3.13.1")
    image = f"rabbitmq:{image_version}"

    with RabbitMqContainer(
        image, username="guest", password="guest", port=Parameters.DEFAULT_PORT
    ) as rabbitmq:
        cl = rabbitmq.get_connection_params()
        host = os.environ.get("RABBITMQ_SERVER", rabbitmq.get_container_host_ip())
        vhost = os.environ.get("RABBITMQ_VHOST", cl.virtual_host)
        queue_id = os.environ.get("RABBITMQ_QUEUE", "xtest")
        q1 = QueueFactory.get_rabbitmq_client(
            queue_id=queue_id,
            host=host,
            vhost=vhost,
            port=rabbitmq.get_exposed_port(Parameters.DEFAULT_PORT),
            durable=False,
        )
        q2 = QueueFactory.get_rabbitmq_client(
            queue_id=queue_id,
            host=host,
            vhost=vhost,
            port=rabbitmq.get_exposed_port(Parameters.DEFAULT_PORT),
            durable=False,
        )

        def finalize():
            q1.close()
            q2.close()

        request.addfinalizer(finalize)
        return q1, q2
