import json
import threading
from typing import Optional

import pika

from app import config

_lock = threading.Lock()
_publisher_connection: Optional[pika.BlockingConnection] = None
_publisher_channel = None


def _connection_params() -> pika.ConnectionParameters:
    credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD)
    return pika.ConnectionParameters(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        virtual_host=config.RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30,
    )


def get_connection() -> pika.BlockingConnection:
    return pika.BlockingConnection(_connection_params())


def declare_topology() -> None:
    """Declare the durable pricing topology.

    Primary messages are consumed from ``pricing.refresh.q``. Failed poison or
    exhausted messages are dead-lettered from that queue to ``pricing.refresh.dlq``.

    Transient retries are implemented by the worker publishing a copy of the
    message to a durable retry queue. The retry queue applies a TTL and then
    dead-letters the message back to the original pricing exchange/routing key,
    which returns it to the unchanged primary queue after the configured delay.
    """

    connection = get_connection()
    try:
        channel = connection.channel()

        channel.exchange_declare(
            exchange=config.PRICING_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )

        channel.exchange_declare(
            exchange=config.PRICING_DLX,
            exchange_type="direct",
            durable=True,
        )
        channel.queue_declare(queue=config.PRICING_DLQ, durable=True)
        channel.queue_bind(
            queue=config.PRICING_DLQ,
            exchange=config.PRICING_DLX,
            routing_key=config.PRICING_DLQ_ROUTING_KEY,
        )

        channel.queue_declare(
            queue=config.PRICING_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": config.PRICING_DLX,
                "x-dead-letter-routing-key": config.PRICING_DLQ_ROUTING_KEY,
            },
        )
        channel.queue_bind(
            queue=config.PRICING_QUEUE,
            exchange=config.PRICING_EXCHANGE,
            routing_key=config.PRICING_ROUTING_KEY,
        )

        channel.exchange_declare(
            exchange=config.PRICING_RETRY_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )
        channel.queue_declare(
            queue=config.PRICING_RETRY_QUEUE,
            durable=True,
            arguments={
                "x-message-ttl": config.RETRY_DELAY_MS,
                "x-dead-letter-exchange": config.PRICING_EXCHANGE,
                "x-dead-letter-routing-key": config.PRICING_ROUTING_KEY,
            },
        )
        channel.queue_bind(
            queue=config.PRICING_RETRY_QUEUE,
            exchange=config.PRICING_RETRY_EXCHANGE,
            routing_key=config.PRICING_RETRY_ROUTING_KEY,
        )
    finally:
        connection.close()


def _ensure_publisher() -> None:
    global _publisher_connection, _publisher_channel
    if _publisher_connection is None or _publisher_connection.is_closed:
        _publisher_connection = get_connection()
        _publisher_channel = _publisher_connection.channel()


def publish_price_refresh(job: dict, correlation_id: str) -> None:
    """Publish a durable JSON price-refresh job to the primary exchange."""

    with _lock:
        _ensure_publisher()
        properties = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            correlation_id=correlation_id,
            headers={"x-retry-count": 0},
        )
        _publisher_channel.basic_publish(
            exchange=config.PRICING_EXCHANGE,
            routing_key=config.PRICING_ROUTING_KEY,
            body=json.dumps(job, separators=(",", ":")).encode("utf-8"),
            properties=properties,
        )


def close_publisher() -> None:
    global _publisher_connection, _publisher_channel
    with _lock:
        if _publisher_connection is not None and _publisher_connection.is_open:
            _publisher_connection.close()
        _publisher_connection = None
        _publisher_channel = None
