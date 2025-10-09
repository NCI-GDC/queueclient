import abc
import logging
import queue
import time
from collections.abc import Callable
from multiprocessing import Queue
from typing import Any

logger = logging.getLogger(__name__)


class QueueClient(abc.ABC):
    def __init__(self, queue_id: str) -> None:
        """An abstract queue for communicating work between managers and worker
        Args:
            queue_id: A reasonable identifier for this queue
        """
        self.is_closing = False
        self.queue_id = queue_id

    def connect(self) -> None:
        """Implement this to initialize the queue for use"""
        ...

    @abc.abstractmethod
    def enqueue(self, msg: Any, durable: bool = True, routing_key: str = "") -> bool:
        """Publishes a message to a queue
        Args:
            msg: JSON serializable object
            durable: if supported by queue, persist data even if service is restarted
            routing_key: useful for selectively focusing on workers'
        Returns:
            True if the action is successful, False otherwise.
        """
        ...

    @abc.abstractmethod
    def dequeue(self, block: bool = False, requeue: bool = True) -> Any:
        """Abstract method to dequeue an item from the queue.

        Args:
            block: Indicates whether the dequeue operation should block if the queue is empty.
            requeue: re-insert the item back into the queue, if a handling exception is raised.

        Returns:
            Any: The item dequeued from the queue.
        """
        ...

    def consume(
        self,
        callback: Callable[[Any], None],
        requeue_failed: bool = True,
        on_failure_callback: Callable[[Any], None] | None = None,
        exit_trigger: Callable[[], bool] | None = None,
    ) -> None:
        """Listens for incoming data in queue, initial impl uses a simple loop that sleeps for
        1 second RabbitMQ uses different implementation.

        Args:
            callback: function in the form
                def callback(msg):
                    do something
            requeue_failed: requeue failed messages
            on_failure_callback: external handling of failed tasks, the same signature as
                callback.
            exit_trigger: callback function that returns True/False used to force the
                consumer to exit.
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
            if self.is_closing or (exit_trigger and exit_trigger()):
                logger.info(f"{self.__class__.__name__}[{self.queue_id}] is shutting down.")
                break
            time.sleep(1)

    def close(self) -> None:
        """Close all connections"""
        self.start_closing()

    def start_closing(self) -> None:
        """Initiate a clean exit from the queue.
        Ensures existing tasks are completed and acknowledged before shutdown."""
        self.is_closing = True

    @abc.abstractmethod
    def status(self) -> bool:
        """Checks the status of the selected queue
        Returns:
            bool: True if queue is active, False otherwise
        """
        ...

    @abc.abstractmethod
    def ping(self) -> None:
        """Used for preliminary verification the queue is usable"""
        ...


class InMemoryQueueClient(QueueClient):
    def __init__(self, queue_id):
        super().__init__(queue_id=queue_id)
        self.q = Queue()

    def connect(self):
        """No-op"""
        pass

    def enqueue(self, msg, durable=False, routing_key="") -> bool:
        if durable:
            raise ValueError("durable functionality is not supported")

        self.q.put(msg)
        return True

    def dequeue(self, block: bool = True, requeue: bool = True) -> Any:
        """Return a message from the queue

        Args:
            block: When true, wait for a message in the queue. Can cause locks when used in a
                separate thread.
            requeue: not used no-op.

        Returns:
            The object in the queue or None
        """
        try:
            return self.q.get(block=block)
        except queue.Empty:
            return None

    def status(self):
        return True

    def ping(self):
        return 1
