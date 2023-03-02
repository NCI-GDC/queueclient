import collections
import logging
import time
from abc import ABCMeta, abstractmethod


class QueueClient:

    __metaclass__ = ABCMeta

    def __init__(self, queue_id):
        """An abstract queue for communicating work between managers and worker
        Args:
            queue_id (str): A reasonable identifier for this queue
        """
        self._is_closing = False
        self.queue_id = queue_id
        self.logger = logging.getLogger(self.__module__ + "." + self.__class__.__name__)

    @abstractmethod
    def connect(self):
        """Implement this to initialize the queue for use"""
        raise NotImplementedError("QueueClient Initialization not implemented")

    @abstractmethod
    def enqueue(self, msg, durable=True, routing_key=""):
        """Publishes a message to a queue
        Args:
            msg (object): JSON serializable object
            durable (bool): if supported by queue, persist data even if service is restarted
            routing_key (str): useful for selectively targeting workers
        """
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def dequeue(self, requeue=True):
        """Blocks and read a single entry from the queue and disconnects
        Returns:
            object: a deserialized object received from queue
        """
        raise NotImplementedError("Method not implemented")

    def consume(self, callback, requeue_failed=True, on_failure_callback=None):
        """Listens for incoming data in queue, initial impl uses a simple loop that sleeps for 1 second
        RabbitMQ uses different implementation
        Args:
            callback: function in the form
                def callback(msg):
                    do something
            requeue_failed (bool): requeue failed messages
            on_failure_callback (function): external handling of failed tasks, same signature as callback
        """
        while True:

            if self._is_closing:
                self._is_closing = False
                break

            msg = self.dequeue()
            if msg:
                try:
                    callback(msg)
                except Exception as e:
                    if requeue_failed:
                        self.enqueue(msg)
                    if on_failure_callback:
                        on_failure_callback(msg)
                    self.logger.error(
                        f"Exception while processing request {e}", exc_info=1
                    )
            time.sleep(1)

    def close(self):
        """Close all connections"""
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
        raise NotImplementedError(
            "Boom Boom !!! QueueClient not implemented properly for use"
        )


class InMemoryQueueClient(QueueClient):
    def __init__(self, queue_id):

        super().__init__(queue_id=queue_id)
        self.q = collections.deque()

    def connect(self):
        pass

    def enqueue(self, msg, durable=False, routing_key=""):
        if durable:
            raise ValueError("durable functionality is not supported")

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
