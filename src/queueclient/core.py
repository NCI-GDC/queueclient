import abc
import logging
import queue
import time
from multiprocessing import Queue
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class QueueClient(abc.ABC):

    def __init__(self, queue_id: str) -> None:
        """An abstract queue for communicating work between managers and worker
        Args:
            queue_id (str): A reasonable identifier for this queue
        """
        self._is_closing = False
        self.queue_id = queue_id

    def connect(self) -> None:
        """Implement this to initialize the queue for use"""
        ...

    @abc.abstractmethod
    def enqueue(self, msg: object, durable: str = True, routing_key: str = "") -> bool:
        """Publishes a message to a queue
        Args:
            msg (object): JSON serializable object
            durable (bool): if supported by queue, persist data even if service is restarted
            routing_key (str): useful for selectively focusing on workers'
        Returns:
            True if the action is successful, False otherwise.
        """
        ...

    @abc.abstractmethod
    def dequeue(self, block: bool, requeue=True) -> Any:
        """Abstract method to dequeue an item from the queue.

        Args:
            block: Indicates whether the dequeue operation should block if the queue is empty.
            requeue: Optional; no-op.

        Returns:
            Any: The item dequeued from the queue.
        """
        ...

    def consume(
        self,
        callback: Callable[[Any], None],
        requeue_failed=True,
        on_failure_callback: Optional[Callable[[Any], None]] = None,
        exit_trigger: Optional[Callable[[], bool]] = None,
    ) -> None:
        """Listens for incoming data in queue, initial impl uses a simple loop that sleeps for 1 second
        RabbitMQ uses different implementation
        Args:
            callback: function in the form
                def callback(msg):
                    do something
            requeue_failed (bool): requeue failed messages
            on_failure_callback (function): external handling of failed tasks, the same signature as callback
            exit_trigger (function): callback function that returns True/False used to force the consumer to exit.
        """

        def __handle_message(message: Any) -> None:
            if not message:
                return

            try:
                callback(message)
            except Exception as e:
                logger.error(f"Exception while processing request {e}", exc_info=e)
                if requeue_failed:
                    self.enqueue(message)
                if on_failure_callback:
                    on_failure_callback(message)

        while True:

            # Do not wait for messages so that the queue can be shut down.
            msg = self.dequeue(block=False)
            __handle_message(msg)

            # attempt to exit the consumer
            if self._is_closing or (exit_trigger and exit_trigger()):
                logger.info(f"{self.__class__.__name__}[{self.queue_id}] is shutting down.")
                break
            time.sleep(1)

    def close(self):
        """Close all connections"""
        self._is_closing = True

    @abc.abstractmethod
    def status(self) -> bool:
        """Checks the status of the selected queue
        Returns:
            bool: True if queue is active, False otherwise
        """
        ...

    @abc.abstractmethod
    def ping(self):
        """Used for preliminary verification the queue is usable"""
        ...


class InMemoryQueueClient(QueueClient):
    def __init__(self, queue_id):
        super().__init__(queue_id=queue_id)
        self.q = Queue()

    def connect(self):
        """No-op"""

    def enqueue(self, msg, durable=False, routing_key="") -> bool:
        if durable:
            raise ValueError("durable functionality is not supported")

        self.q.put(msg)
        return True

    def dequeue(self, block=True, requeue: bool = True):
        """Return a message from the queue

        Args:
            block (bool, optional): When true, wait for a message on the queue. Can cause locks when used in a separate thread.
            requeue: Optional; no-op.

        Returns:
            Optional<Any>: The object in the queue or None
        """
        try:
            return self.q.get(block=block)
        except queue.Empty:
            return None

    def status(self):
        return True

    def ping(self):
        return 1
