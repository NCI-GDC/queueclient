import requests
from requests import HTTPError

from queueclient.core import QueueClient


class DepotQueueClient(QueueClient):
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
            self.logger.error(e.message, exc_info=1)
        return False

    def enqueue(self, msg, durable=False, routing_key=""):
        """Submits a JSON object to Depot Server
        Args:
            msg (object): JSON object
            durable (bool): Not supported by server
            routing_key (str): unused attrib
        Returns:
            bool: True if task was submitted successfully
        """
        if durable:
            raise ValueError("durable functionality is not supported")

        try:
            url = f"{self.depot_server_url}/delegate/{self.queue_id}"
            response = requests.put(url, json=msg)
            return response.status_code == 200
        except HTTPError as e:
            self.logger.error(e.message, exc_info=1)
        return False

    def dequeue(self, requeue=True, block=False):
        """Retrieves a single JSON object from Depot Server
        Returns:
            object: JSON object
        """
        if block:
            self.logger.warning("Blocking is not available in the DepotQueueClient")
        try:
            url = f"{self.depot_server_url}/work/{self.queue_id}"
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
        except HTTPError as e:
            self.logger.error(e, exc_info=1)
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
            raise HTTPError(
                f"Boom Boom !!!, Depot QueueClient not reachable @ {ping_url}"
            )

        message = response.json()
        self.logger.info("Using Depot Version: {}".format(message["version"]))
