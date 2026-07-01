import functools
import logging
import threading
from collections.abc import Callable
from typing import Any

import pika
import simplejson as json
import tenacity
from pika import channel, connection, frame, spec

from queueclient import core

logger = logging.getLogger(__name__)


def should_retry(e: BaseException) -> bool:
    """Handles all the connection errors that may occur."""
    if isinstance(e, pika.exceptions.StreamLostError):
        return True
    if isinstance(e, ConnectionError):
        return True

    logger.exception("Do not retry: %r", e)
    return False


def log_failure(retry_state: tenacity.RetryCallState):
    """tenacity log helper.

    To be called on final failure with `after`
    """
    if retry_state.outcome.failed:
        exc = retry_state.outcome.exception()
        logger.info(
            "Tenacity (fn=%r) reports failure after %d attempts, elapsed=%.2fs: %s",
            retry_state.fn,
            retry_state.attempt_number,
            retry_state.seconds_since_start,
            exc,
        )


def log_final_failure(retry_state: tenacity.RetryCallState):
    """tenacity log helper.

    To be called on final failure with `after`
    """
    if retry_state.outcome.failed:
        exc = retry_state.outcome.exception()
        logger.exception(
            "Tenacity (fn=%r) reports failure after %d attempts, elapsed=%.2fs: %s."
            " Cannot continue",
            retry_state.fn,
            retry_state.attempt_number,
            retry_state.seconds_since_start,
            exc,
            exc_info=exc,
        )


class RabbitMQClient(core.QueueClient):
    """
    RabbitMQ implmentation of the QueueClient.

    Terms:
        qos: quality of service. controls how many messages can be
             sent to a consumer without acknowledgement
    """

    def __init__(
        self,
        host="localhost",
        vhost="/dev",
        port=5672,
        queue_id="dev",
        username="guest",
        password="guest",
        durable=True,
        exchange=None,
        exchange_type=None,
        routing_key=None,
        # We have long running jobs (terabyte sized transfer) which blocks the heartbeats.
        # A failed heartbeat will cause the server to drop the client.
        # Don't use a short heartbeat unless you need it.
        heartbeat=3600,
    ):
        """
        Args:
            host (str): RabbitMQ server host or IP
            vhost (str): Virtual host defined on RabbitMQ
            port (int): defaults to 5672, the default rabbitmq port
            queue_id (str): queue name to bind to, created if it does not exist
            username (str): username
            password (str): password
            durable (bool): durable queues survive server restarts
        """

        super().__init__(queue_id=queue_id)

        self.host = host
        self.port = port
        self.v_host = vhost

        self.durable = durable
        self.heartbeat = heartbeat
        self.username = username
        self.password = password

        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key

        self.channel = None
        self.connection = None

        self.consumer_tag = None

        self._callback = None
        self._on_failure_callback = None
        self._terminate_consumer_callback = None
        self._requeue_failed = True
        self._is_consuming = False

        credentials = pika.PlainCredentials(username, password, erase_on_connect=True)
        self.conn_params = pika.ConnectionParameters(
            host=host,
            heartbeat=self.heartbeat,
            blocked_connection_timeout=300,
            port=port,
            virtual_host=vhost,
            credentials=credentials,
        )

        # Optional: maximum time spent retrying a single reconnect sequence
        self.reconnect_max_delay_seconds = 300  # 5 minutes

        self.client: RabbitMQClient | None = None

    def connect(self):
        raise RuntimeError("Use one of the queuing/consumer methods to connect")

    def _build_publish_retry(self):
        return tenacity.retry(
            wait=tenacity.wait_exponential(multiplier=0.5, min=0.5, max=5),
            stop=tenacity.stop_after_delay(self.reconnect_max_delay_seconds),
            retry=tenacity.retry_if_exception(should_retry),
            reraise=True,
            after=log_failure,
            retry_error_callback=log_final_failure,
        )

    def _ensure_publisher(self):
        if not (isinstance(self.client, RabbitPublisher) and self.status()):
            self.client = RabbitPublisher(
                self.host,
                self.v_host,
                self.port,
                self.queue_id,
                self.username,
                self.password,
                self.durable,
                self.exchange,
                self.exchange_type,
                self.routing_key,
            )
            self.client.connect()

    @staticmethod
    def _close_publisher(client: "RabbitPublisher | None") -> None:
        if client:
            try:
                client.close()
            except Exception:
                logger.debug("Error closing publisher during retry", exc_info=True)

    def enqueue(
        self,
        msg: core.TMessage,
        durable: bool = True,
        routing_key: str | None = None,
        serialize: Callable[[core.TMessage], str] = json.dumps,
    ) -> bool:
        """Publish a message to queue and keeps connection open.

        Args:
            msg: message to publish
            durable: if supported by queue, persist data even if service is restarted
            routing_key: useful for selectively focusing on workers'
            serialize: Converts the msg argument into a UTF-8 json string
        Returns:
            True if the action is successful, False otherwise.
        """
        retry_decorator = self._build_publish_retry()
        logger.info("Retry decorator: %r", retry_decorator)

        @retry_decorator
        def do_publish():
            self._ensure_publisher()
            rk = routing_key or self.routing_key or self.queue_id
            try:
                return self.client.basic_publish(msg, durable, rk, serialize=serialize)
            except Exception as e:
                if should_retry(e):
                    logger.info(
                        "Retryable exception %r while publishing to queue %s; "
                        "will retry via tenacity",
                        e,
                        self.queue_id,
                    )
                    RabbitMQClient._close_publisher(self.client)
                    self.client = None
                else:
                    logger.exception(
                        "Non-retryable exception while publishing to %s", self.queue_id
                    )
                raise  # always re-raise

        do_publish()
        return True

    def consume(
        self,
        callback: Callable[[Any], None],
        requeue_failed=True,
        on_failure_callback: Callable[[Any], None] | None = None,
        exit_trigger: Callable[[], bool] | None = None,
    ) -> None:
        """Listens for incoming data in queue
        Args:
            callback: function in the form
                def callback(body: str) -> None:
                    `Args`:
                        body: response retrieved from queue`
                    do something
            requeue_failed: If True requeue task on failure.
            on_failure_callback: external handling of failed tasks, the same signature as
                callback.
            exit_trigger: callback function that returns True/False used to force the consumer
                to exit.
        """

        self.client = RabbitConsumer(
            self.host,
            self.v_host,
            self.port,
            self.queue_id,
            self.username,
            self.password,
            self.durable,
            self.exchange,
            self.exchange_type,
            self.routing_key,
        )
        self.client._callback = callback
        self.client._requeue_failed = requeue_failed
        self.client._on_failure_callback = on_failure_callback
        self.client._terminate_consumer_callback = exit_trigger

        self.client.connect()
        self.client.start()

    def dequeue(
        self,
        block=True,
        requeue=True,
        deserialize: Callable[[str], core.TMessage] = json.loads,
    ) -> core.TMessage:
        """Opens a new connection, performs consume and closes connection, returns response."""

        self.client = RabbitPublisher(
            self.host,
            self.v_host,
            self.port,
            self.queue_id,
            self.username,
            self.password,
            self.durable,
            self.exchange,
            self.exchange_type,
            self.routing_key,
        )

        self.client.connect()
        body = self.client.basic_get(requeue, deserialize=deserialize)
        self.client.close()
        return body

    def status(self) -> bool:
        if not self.client:
            return False
        conn = getattr(self.client, "connection", None)
        chan = getattr(self.client, "channel", None)
        return bool(conn and chan and conn.is_open and chan.is_open)

    def ping(self) -> bool:
        """Not required"""
        if self.client.channel is None:
            return False

        return True

    def close(self):
        if self.client:
            self.client.close()
            self.client = None  # remove stale client names

    def start_closing(self) -> None:
        if self.client:
            self.client.is_closing = True


class RabbitConsumer(RabbitMQClient):
    """RabbitMQ consumer with tenacity-based reconnection."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Keeps track of whether we are intentionally shutting down
        self.is_closing = False

    def connect(self):
        """Establish a new SelectConnection."""
        logger.info(
            "Connecting to RabbitMQ: host=%s, vhost=%s, queue=%s",
            self.host,
            self.v_host,
            self.queue_id,
        )
        self.is_closing = False
        self.connection = pika.SelectConnection(
            self.conn_params,
            on_open_callback=self.on_connection_open,
            on_close_callback=self.on_connection_closed,
            on_open_error_callback=self.on_connection_open_error,
        )

    def on_connection_closed(self, _conn: connection.Connection, reason: Exception) -> None:
        self.channel = None
        logger.warning(
            "Connection closed for queue %s: %r (is_closing=%s)",
            self.queue_id,
            reason,
            self.is_closing,
        )
        if self.is_closing:
            # closing is intentional
            try:
                self.connection.ioloop.stop()
            except Exception:
                logger.debug("ioloop.stop() raised but will be ignored", exc_info=True)
        else:
            # Unintentional close: schedule reconnect with tenacity
            self._schedule_reconnect(reason=reason)

    def on_connection_open_error(self, _unused_connection, err):
        logger.error("Connection open failed: %s - %s", err, self.queue_id)
        # Use tenacity-based reconnect
        self._schedule_reconnect(reason=err)

    def on_connection_open(self, _conn):
        logger.info("Connection opened to %s", _conn.params.host)
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, msg_channel: channel.Channel) -> None:
        logger.info("Channel opened for queue %s", self.queue_id)
        self.channel = msg_channel
        self.channel.add_on_close_callback(self.on_channel_closed)

        # setup exchange
        if self.exchange and self.exchange_type:
            self.channel.exchange_declare(
                exchange=self.exchange,
                durable=self.durable,
                exchange_type=self.exchange_type,
                callback=self.on_exchange_declare_ok,
            )
        else:
            self.channel.queue_declare(
                queue=self.queue_id,
                durable=self.durable,
                callback=self.on_queue_declare_ok,
            )

    def on_channel_closed(self, msg_channel: channel.Channel, reason: Exception) -> None:
        self.channel = None
        logger.debug(
            "Channel '%s' closed for queue %s: %r",
            msg_channel.channel_number,
            self.queue_id,
            reason,
        )
        if (
            self.is_closing
            and self.connection
            and not self.connection.is_closing
            and not self.connection.is_closed
        ):
            self.connection.close()

    def on_exchange_declare_ok(self, _header):
        self.channel.queue_declare(
            queue=self.queue_id, durable=self.durable, callback=self.on_queue_declare_ok
        )

    def on_queue_declare_ok(self, _header):
        if self.exchange:
            self.channel.queue_bind(
                queue=self.queue_id,
                exchange=self.exchange,
                routing_key=self.routing_key,
                callback=self.on_bind_ok,
            )
        else:
            self.channel.basic_qos(prefetch_count=1, callback=self.on_basic_qos_ok)

    def on_bind_ok(self, _header):
        self.channel.basic_qos(prefetch_count=1, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _header):
        # start consuming
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)
        on_basic_consume = functools.partial(
            self._basic_callback,
            callback=self._callback,
            requeue_failed=self._requeue_failed,
            on_failure=self._on_failure_callback,
        )
        self.consumer_tag = self.channel.basic_consume(
            queue=self.queue_id, on_message_callback=on_basic_consume
        )
        self._is_consuming = True
        logger.info("Started consuming on queue %s", self.queue_id)

    def on_consumer_cancelled(self, _frame):
        if self.channel:
            self.channel.close()
        logger.error(
            "RabbitConsumer channel closed unexpectedly for queue %s: %s",
            self.queue_id,
            _frame,
        )

    def close(self):
        self.is_closing = True
        if self.channel and not self.channel.is_closed and not self.channel.is_closing:
            self.channel.close()
        if (
            self.connection
            and not self.connection.is_closed
            and not self.connection.is_closing
        ):
            self.connection.close()

    def _basic_callback(
        self,
        msg_channel: channel.Channel,
        method: spec.Basic.Deliver,
        _: spec.BasicProperties,
        body: bytes,
        callback: Callable[[str], None],
        requeue_failed: bool = True,
        on_failure: Callable[[str], None] | None = None,
    ):
        """Wraps user provided callback & adds basic acknowledgement when nothing goes wrong"""

        try:
            # py3 returns bytes
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            callback(body)
            msg_channel.basic_ack(method.delivery_tag)
        except Exception as e:
            msg_channel.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue_failed)
            if on_failure:
                on_failure(body)
            logger.error(f"Exception while processing request {e}", exc_info=e)

        if self.is_closing or (
            self._terminate_consumer_callback and self._terminate_consumer_callback()
        ):
            logger.info(f"{self.__class__.__name__}[{self.queue_id}] is shutting down.")
            self.stop()

    def start(self):
        """Start the ioloop; this call blocks until the loop stops."""
        logger.info("Starting RabbitConsumer ioloop for queue %s", self.queue_id)
        self.connection.ioloop.start()
        logger.info("RabbitConsumer ioloop stopped for queue %s", self.queue_id)

    def stop(self):
        """Initiate a (graceful) shutdown of the consumer."""
        logger.info("Stopping RabbitConsumer for queue %s", self.queue_id)
        self.is_closing = True
        if self._is_consuming and self.channel and self.channel.is_open:
            # Cancel current consumer; on_cancel_ok will close channel / stop loop
            self.channel.basic_cancel(
                consumer_tag=self.consumer_tag, callback=self.on_cancel_ok
            )
        else:
            # No active consumer or channel; stop ioloop directly
            if self.connection and not self.connection.is_closed:
                self.connection.ioloop.stop()

    def on_cancel_ok(self, _: frame.Method) -> None:
        self._is_consuming = False
        if self.channel and self.channel.is_open:
            self.channel.close()
        if self.connection and not self.connection.is_closed:
            self.connection.ioloop.stop()

    # -------------------------------------------------------------------------
    # Tenacity-based reconnect
    # -------------------------------------------------------------------------
    def _build_reconnect_retry(self):
        """Return a tenacity retry decorator configured for reconnects."""
        return tenacity.retry(
            wait=tenacity.wait_exponential(multiplier=1, min=1, max=30),
            stop=tenacity.stop_after_delay(self.reconnect_max_delay_seconds),
            retry=tenacity.retry_if_exception(should_retry),
            reraise=True,
            after=log_failure,
            retry_error_callback=log_final_failure,
        )

    def _reconnect_once(self):
        """Perform one reconnect attempt: connect + start ioloop."""
        # Note: this method is deliberately small; tenacity will retry it.
        if self.is_closing:
            # If we're intentionally closing, do not reconnect
            logger.info(
                "Not reconnecting RabbitConsumer for queue %s (is_closing=True)",
                self.queue_id,
            )
            return

        logger.info("Attempting reconnect for queue %s", self.queue_id)
        self.connect()
        # This will block until ioloop stops again (either normal stop or another failure)
        self.start()

    def _schedule_reconnect(self, reason: Exception | None = None):
        """Stop current loop and spawn a thread that will retry reconnect via tenacity."""
        if self.is_closing:
            logger.info(
                "Not scheduling reconnect for queue %s; closing is intentional",
                self.queue_id,
            )
            return

        logger.warning("Scheduling reconnect for queue %s; reason=%r", self.queue_id, reason)

        # Stop current ioloop safely
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.ioloop.stop()
        except Exception:
            logger.debug("ioloop.stop() raised but will be ignored", exc_info=True)

        retry_decorator = self._build_reconnect_retry()

        @retry_decorator
        def reconnect_with_retry():
            self._reconnect_once()

        # Run reconnect attempts in a background thread so we don't block any caller.
        def run():
            try:
                reconnect_with_retry()
            except tenacity.RetryError as exc:
                # All retries exhausted
                logger.error(
                    "RabbitConsumer reconnect attempts exhausted for queue %s; last error: %s",
                    self.queue_id,
                    exc.last_attempt.exception() if exc.last_attempt else exc,
                )
            except Exception:
                logger.exception(
                    "Unexpected error while reconnecting RabbitConsumer for queue %s",
                    self.queue_id,
                )

        threading.Thread(target=run, daemon=True).start()


class RabbitPublisher(RabbitMQClient):
    """Used for publishing to task queue"""

    def connect(self):
        """Initializes connection to RabbitMQ and does queue to exchange binding"""

        self.connection = pika.BlockingConnection(self.conn_params)
        self.channel = self.connection.channel()

        self.channel.queue_declare(self.queue_id, durable=self.durable)

        if self.exchange and self.exchange_type and self.routing_key:
            self.channel.exchange_declare(
                exchange=self.exchange,
                exchange_type=self.exchange_type,
                durable=self.durable,
            )
            self.channel.queue_bind(
                queue=self.queue_id,
                exchange=self.exchange,
                routing_key=self.routing_key,
            )
        logger.debug(f"Blocking Connection established to {self.conn_params}")

    def close(self):
        self.is_closing = True
        if self.connection and self.connection.is_open:
            self.connection.close()
        self.connection = None
        self.channel = None

    def basic_publish(
        self,
        msg: core.TMessage,
        durability: bool,
        routing_key: str | None,
        serialize: Callable[[core.TMessage], str] = json.dumps,
    ):
        """
        Publish with automatic retry on StreamLostError.
        Each retry will re-establish the BlockingConnection if needed.
        """
        # (Re)connect if connection is not usable
        if not (
            self.connection
            and self.connection.is_open
            and self.channel
            and self.channel.is_open
        ):
            logger.warning("Publisher connection not open; reconnecting before publish.")

            if self.connection is not None and not self.connection.is_closed:
                try:
                    self.connection.close()
                except Exception:
                    logger.debug("Error closing dead connection")
            self.connection = None
            self.channel = None
            self.connect()

        msg = serialize(msg)
        delivery_mode = 2 if durability else 1  # mode 2 == durable, 1 == not durable
        props = pika.BasicProperties(
            delivery_mode=delivery_mode,
            content_type="application/json",
            content_encoding="utf-8",
        )

        exchange = self.exchange or ""  # default exchange if none specified
        routing_key = routing_key or self.routing_key or self.queue_id

        return self.channel.basic_publish(
            exchange, routing_key=routing_key, body=msg, properties=props
        )

    def basic_get(
        self, requeue=True, deserialize: Callable[[str], core.TMessage] = json.loads
    ):
        mtd, _, body = self.channel.basic_get(self.queue_id)

        if body:
            try:
                # py3 returns bytes
                if isinstance(body, bytes):
                    body = body.decode("utf-8")
                body = deserialize(body)

                # acknowledge receipt
                self.channel.basic_ack(delivery_tag=mtd.delivery_tag)
            except Exception as e:
                # requeue failure
                self.channel.basic_nack(delivery_tag=mtd.delivery_tag, requeue=requeue)
                raise e
        return body
