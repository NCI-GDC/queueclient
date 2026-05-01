import functools
import logging
from collections.abc import Callable
from typing import Any

import pika
import simplejson as json
from pika import channel, connection, frame, spec

from queueclient import core

logger = logging.getLogger(__name__)


class RabbitMQClient(core.QueueClient):
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
        heartbeat=60,  # seconds between connection checks, 60 is default value
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

        self.client: RabbitMQClient | None = None

    def connect(self):
        raise RuntimeError("Use one of the queuing/consumer methods to connect")

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
        routing_key = routing_key or self.routing_key or self.queue_id
        self.client.basic_publish(msg, durable, routing_key, serialize=serialize)
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
        if self.client:
            return self.client.channel.is_open
        return False

    def ping(self) -> bool:
        """Not required"""
        if self.client.channel is None:
            return False

        return True

    def close(self):
        if self.client:
            self.client.close()

    def start_closing(self) -> None:
        if self.client:
            self.client.is_closing = True


class RabbitConsumer(RabbitMQClient):
    def connect(self):
        self.connection = pika.SelectConnection(
            self.conn_params,
            on_open_callback=self.on_connection_open,
            on_close_callback=self.on_connection_closed,
            on_open_error_callback=self.on_connection_open_error,
        )

    def on_connection_closed(self, _conn: connection.Connection, reason: Exception) -> None:
        self.channel = None
        logger.debug(f"Connection '{_conn.params.host}'", exc_info=reason)
        if self.is_closing:
            # closing is intentional
            self.connection.ioloop.stop()
        else:
            # closing not intentional, try to schedule restart
            self.stop()

    def on_connection_open_error(self, _unused_connection, err):
        logger.error("Connection open failed: %s - %s", err, self.queue_id)

    def on_connection_open(self, _conn):
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, msg_channel: channel.Channel) -> None:
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
        if (
            self.is_closing
            and not self.connection.is_closing
            and not self.connection.is_closed
        ):
            self.connection.close()
        logger.debug("Channel '%s' closed:", msg_channel.channel_number, exc_info=reason)

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

    def on_consumer_cancelled(self, _frame):
        if self.channel:
            self.channel.close()
        logger.error("RabbitConsumer channel closed unexpectedly: %s", _frame)

    def close(self):
        self.is_closing = True
        if self.channel is not None:
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
        self.connection.ioloop.start()

    def stop(self):
        self.is_closing = True
        if self._is_consuming:
            self.channel.basic_cancel(
                consumer_tag=self.consumer_tag, callback=self.on_cancel_ok
            )
        else:
            self.connection.ioloop.stop()

    def on_cancel_ok(self, _: frame.Method) -> None:
        self._is_consuming = False
        if self.channel and self.channel.is_open:
            self.channel.close()


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

    def basic_publish(
        self,
        msg: core.TMessage,
        durability: bool,
        routing_key: str | None,
        serialize: Callable[[core.TMessage], str] = json.dumps,
    ):
        msg = serialize(msg)
        delivery_mode = 2 if durability else 1  # mode 2 == durable, 1 == not durable
        props = pika.BasicProperties(
            delivery_mode=delivery_mode,
            content_type="application/json",
            content_encoding="utf-8",
        )

        try:
            exchange = self.exchange or ""  # use default exchange if no exchange is specified
            routing_key = routing_key or self.routing_key or self.queue_id  #
            return self.channel.basic_publish(
                exchange, routing_key=routing_key, body=msg, properties=props
            )
        except pika.exceptions.UnroutableError as e:
            logger.error(
                f"Message could not be routed to queue with error {e}",
                exc_info=e,
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

    def close(self):
        self.is_closing = True
        if self.connection and self.connection.is_open:
            self.connection.close()
