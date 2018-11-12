import collections
import logging
import time
from abc import ABCMeta, abstractmethod


class QueueClient(object):

    __metaclass__ = ABCMeta

    def __init__(self, queue_id):
        """ An abstract queue for communicating work between managers and worker
        Args:
            queue_id (str): A reasonable identifier for this queue
        """
        self._is_closing = False
        self.queue_id = queue_id
        self.logger = logging.getLogger(self.__module__ + "." + self.__class__.__name__)

    @abstractmethod
    def setup(self):
        """Implement to initialize the queue for use"""
        raise NotImplementedError("QueueClient Initialization not implemented")

    @abstractmethod
    def enqueue(self, msg, durable=True):
        """ Publishes a message to a queue """
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def dequeue(self):
        """ Blocks and read a single entry from the queue and disconnects"""
        raise NotImplementedError("Method not implemented")

    def consume(self, callback):
        """ Listens for incoming data in queue
            Args:
                callback: function in the form
                    def callback(msg):
                        do something
        """
        while True:

            if self._is_closing:
                self._is_closing = False
                break

            msg = self.dequeue()
            if msg:
                callback(msg)
            time.sleep(5)

    def close(self):
        """ Close all connections """
        self._is_closing = True

    @abstractmethod
    def status(self):
        """Checks the status of the selected queue
        Returns:
            bool: True if queue is active, False otherwise
        """
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def ping(self):
        """Used for preliminary verification the queue is usable"""
        raise NotImplementedError("Boom Boom !!! QueueClient not implemented properly for use")


class InMemoryQueueClient(QueueClient):

    def __init__(self, queue_id):

        super(InMemoryQueueClient, self).__init__(queue_id=queue_id)
        self.q = collections.deque()

    def setup(self):
        pass

    def enqueue(self, msg, durable=False):
        self.q.appendleft(msg)
        return True

    def dequeue(self):
        if len(self.q) > 0:
            return self.q.pop()
        return None

    def status(self):
        return len(self.q) >= 0

    def ping(self):
        return 1
