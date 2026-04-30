import logging
from collections.abc import Callable

import requests
import simplejson as json
from deprecated import deprecated
from requests import HTTPError

from queueclient import core

logger = logging.getLogger(__name__)


@deprecated(reason="Depot is no longer maintained internally.")
class DepotQueueClient(core.QueueClient):
    def __init__(self, queue_id, host="depot.service.consul", port=80, version="v0"):
        super().__init__(queue_id=queue_id)

        self._is_closing = False
        self.depot_server_url = f"http://{host}:{port}/{version}"

        self.connect()
        self.ping()

    def connect(self):
        if self.status() is False:
            self._create()

    def status(self):
        try:
            url = f"{self.depot_server_url}/status/{self.queue_id}"
            response = requests.get(url)
            return response.status_code == 200
        except HTTPError as e:
            logger.error("HTTP error", exc_info=e)
        return False

    def enqueue(
        self,
        msg: core.TMessage,
        durable: bool = False,
        routing_key: str = "",
        serialize: Callable[[core.TMessage], str] = json.dumps,
    ) -> bool:
        """Submits a JSON object to Depot Server
        Args:
            msg: JSON object
            durable: Not supported by server
            routing_key: unused attrib
            serialize: unused

        Returns:
            True if task was submitted successfully
        """
        if durable:
            raise ValueError("durable functionality is not supported")

        try:
            url = f"{self.depot_server_url}/delegate/{self.queue_id}"
            response = requests.put(url, json=msg)
            return response.status_code == 200
        except HTTPError as e:
            logger.error("HTTP Error", exc_info=e)
        return False

    def dequeue(
        self,
        requeue: bool = True,
        block: bool = False,
        deserialize: Callable[[str], core.TMessage] = json.loads,
    ):
        """Retrieves a single JSON object from Depot Server

        Args:
            requeue: unused
            block: not available
            deserialize: unused

        Returns:
            object: JSON object
        """
        if block:
            logger.warning("Blocking is not available in the DepotQueueClient")
        try:
            url = f"{self.depot_server_url}/work/{self.queue_id}"
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
        except HTTPError as e:
            logger.error("Depot http error while de-queuing.", exc_info=e)
        return None

    def clear(self):
        r = requests.put(f"{self.depot_server_url}/clear/{self.queue_id}")
        return r.status_code == 200

    def _create(self):
        url = f"{self.depot_server_url}/new/{self.queue_id}"
        response = requests.put(url)
        if response.status_code == 200:
            return True
        raise HTTPError("Depot QueueClient could not be setup correctly")

    def ping(self):
        ping_url = f"{self.depot_server_url}/"
        response = requests.get(url=ping_url)
        if response.status_code != 200:
            raise HTTPError(f"Boom Boom !!!, Depot QueueClient not reachable @ {ping_url}")

        message = response.json()
        logger.info("Using Depot Version: {}".format(message["version"]))
