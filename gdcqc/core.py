import collections
import logging
from abc import ABCMeta, abstractmethod


class ServiceQueue(object):

    __metaclass__ = ABCMeta

    def __init__(self, queue_id):
        """ An abstract queue for communicating work between managers and worker
        Args:
            queue_id (str): A reasonable identifier for this queue
        """
        self.queue_id = queue_id
        self.logger = logging.getLogger(self.__module__ + "." + self.__class__.__name__)
        self.ping()
        self.setup()

    @abstractmethod
    def setup(self):
        """Implement to initialize the queue for use"""
        raise NotImplemented("ServiceQueue Initialization not implemented")

    @abstractmethod
    def enqueue(self, msg):
        raise NotImplemented("Method not implemented")

    @abstractmethod
    def dequeue(self):
        raise NotImplemented("Method not implemented")

    @abstractmethod
    def status(self):
        """Checks the status of the selected queue
        Returns:
            bool: True if queue is active, False otherwise
        """
        raise NotImplemented("Method not implemented")

    @abstractmethod
    def ping(self):
        """Used for preliminary verification the queue is usable"""
        raise NotImplemented("Boom Boom !!! ServiceQueue not implemented properly for use")


class InMemoryQueue(ServiceQueue):

    def __init__(self, queue_id):

        super(InMemoryQueue, self).__init__(queue_id=queue_id)
        self.q = collections.deque()

    def setup(self):
        pass

    def enqueue(self, msg):
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
