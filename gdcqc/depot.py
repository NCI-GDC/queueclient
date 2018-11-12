import requests
from requests import HTTPError

from gdcqc.core import ServiceQueue


class DepotServiceQueue(ServiceQueue):

    def __init__(self, depot_url, queue_id):

        super(DepotServiceQueue, self).__init__(queue_id=queue_id)

        self._is_closing = False
        self.depot_server_url = depot_url

        self.setup()
        self.ping()

    def setup(self):
        if self.status() is False:
            self._create()

    def status(self):
        try:
            url = "{}/status/{}".format(self.depot_server_url, self.queue_id)
            response = requests.get(url)
            return response.status_code == 200
        except HTTPError as e:
            self.logger.error(e.message, exc_info=1)
        return False

    def enqueue(self, msg, durable=False):
        try:
            url = "{}/delegate/{}".format(self.depot_server_url, self.queue_id)
            response = requests.put(url, json=msg)
            return response.status_code == 200
        except HTTPError as e:
            self.logger.error(e.message, exc_info=1)
        return False

    def dequeue(self):
        try:
            url = "{}/work/{}".format(self.depot_server_url, self.queue_id)
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
        except HTTPError as e:
            self.logger.error(e.message, exc_info=1)
        return None

    def _create(self):
        url = "{}/new/{}".format(self.depot_server_url, self.queue_id)
        response = requests.put(url)
        if response.status_code == 200:
            return True
        raise HTTPError("Depot ServiceQueue could not be setup correctly")

    def ping(self):

        ping_url = "{}/".format(self.depot_server_url)
        response = requests.get(url=ping_url)
        if response.status_code != 200:
            raise HTTPError("Boom Boom !!!, Depot ServiceQueue not reachable @ {}".format(ping_url))

        message = response.json()
        self.logger.info("Using Depot Version: {}".format(message["version"]))
