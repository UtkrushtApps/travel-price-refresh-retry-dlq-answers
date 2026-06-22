import json
import logging
import time
from typing import Any, Optional

import pika

from app import config
from app.rabbitmq import declare_topology, get_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pricing.worker")


class PermanentJobError(Exception):
    pass


class TransientJobError(Exception):
    pass


def refresh_supplier_prices(job: dict) -> dict:
    suppliers = job.get("supplier_codes", [])
    if not suppliers:
        raise PermanentJobError("no supplier codes provided")
    if "FLAKY" in suppliers:
        raise TransientJobError("supplier temporarily unavailable")
    time.sleep(0.2)
    return {"itinerary_id": job.get("itinerary_id"), "priced_suppliers": suppliers}


def _headers(properties: Optional[pika.BasicProperties]) -> dict:
    if properties is None or properties.headers is None:
        return {}
    return dict(properties.headers)


def _retry_count(properties: Optional[pika.BasicProperties]) -> int:
    raw_value = _headers(properties).get("x-retry-count", 0)
    if isinstance(raw_value, bytes):
        raw_value = raw_value.decode("utf-8", errors="ignore")
    try:
        return max(0, int(raw_value))
    except (TypeError, ValueError):
        return 0


def _copy_properties_for_retry(
    properties: Optional[pika.BasicProperties], retry_count: int
) -> pika.BasicProperties:
    """Copy application-visible metadata while updating the retry counter.

    RabbitMQ will preserve these properties when the retry queue dead-letters the
    message back to the primary exchange after its TTL expires. We explicitly set
    content_type to application/json and retain correlation_id so downstream logs
    and consumers see stable metadata across redeliveries.
    """

    headers = _headers(properties)
    headers["x-retry-count"] = retry_count

    return pika.BasicProperties(
        content_type="application/json",
        content_encoding=getattr(properties, "content_encoding", None),
        headers=headers,
        delivery_mode=2,
        priority=getattr(properties, "priority", None),
        correlation_id=getattr(properties, "correlation_id", None),
        reply_to=getattr(properties, "reply_to", None),
        message_id=getattr(properties, "message_id", None),
        timestamp=getattr(properties, "timestamp", None),
        type=getattr(properties, "type", None),
        user_id=getattr(properties, "user_id", None),
        app_id=getattr(properties, "app_id", None),
        cluster_id=getattr(properties, "cluster_id", None),
    )


def _dead_letter(channel: pika.channel.Channel, delivery_tag: int, reason: str) -> None:
    logger.error("dead-lettering message: %s", reason)
    channel.basic_nack(delivery_tag=delivery_tag, requeue=False)


def _publish_retry(
    channel: pika.channel.Channel,
    body: bytes,
    properties: Optional[pika.BasicProperties],
    next_retry_count: int,
) -> None:
    retry_properties = _copy_properties_for_retry(properties, next_retry_count)
    channel.basic_publish(
        exchange=config.PRICING_RETRY_EXCHANGE,
        routing_key=config.PRICING_RETRY_ROUTING_KEY,
        body=body,
        properties=retry_properties,
    )


def _decode_job(properties: Optional[pika.BasicProperties], body: bytes) -> dict[str, Any]:
    if getattr(properties, "content_type", None) != "application/json":
        raise PermanentJobError("message content_type must be application/json")

    try:
        decoded = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise PermanentJobError("could not decode JSON message body") from exc

    if not isinstance(decoded, dict):
        raise PermanentJobError("message body must be a JSON object")

    supplier_codes = decoded.get("supplier_codes")
    if not isinstance(supplier_codes, list) or not supplier_codes:
        raise PermanentJobError("supplier_codes must be a non-empty list")
    if not all(isinstance(code, str) and code for code in supplier_codes):
        raise PermanentJobError("supplier_codes must contain non-empty strings")

    if not isinstance(decoded.get("job_id"), str) or not decoded["job_id"]:
        raise PermanentJobError("job_id must be a non-empty string")
    if not isinstance(decoded.get("itinerary_id"), str) or not decoded["itinerary_id"]:
        raise PermanentJobError("itinerary_id must be a non-empty string")
    if not isinstance(decoded.get("currency"), str) or len(decoded["currency"]) != 3:
        raise PermanentJobError("currency must be a three-letter string")

    return decoded


def _on_message(channel, method, properties, body) -> None:
    delivery_tag = method.delivery_tag

    try:
        job = _decode_job(properties, body)
    except PermanentJobError as exc:
        _dead_letter(channel, delivery_tag, str(exc))
        return

    try:
        result = refresh_supplier_prices(job)
    except PermanentJobError as exc:
        logger.warning("permanent failure for job %s: %s", job.get("job_id"), exc)
        _dead_letter(channel, delivery_tag, str(exc))
        return
    except Exception as exc:
        current_retry_count = _retry_count(properties)
        if current_retry_count >= config.MAX_RETRIES:
            logger.warning(
                "job %s exhausted retries (%s/%s): %s",
                job.get("job_id"),
                current_retry_count,
                config.MAX_RETRIES,
                exc,
            )
            _dead_letter(channel, delivery_tag, "retry attempts exhausted")
            return

        next_retry_count = current_retry_count + 1
        logger.warning(
            "job %s failed: %s; scheduling retry %s/%s in %sms",
            job.get("job_id"),
            exc,
            next_retry_count,
            config.MAX_RETRIES,
            config.RETRY_DELAY_MS,
        )
        _publish_retry(channel, body, properties, next_retry_count)
        channel.basic_ack(delivery_tag=delivery_tag)
        return

    logger.info("priced job %s -> %s", job.get("job_id"), result)
    channel.basic_ack(delivery_tag=delivery_tag)


def main() -> None:
    declare_topology()
    connection = get_connection()
    channel = connection.channel()
    channel.basic_qos(prefetch_count=config.PREFETCH_COUNT)
    channel.basic_consume(
        queue=config.PRICING_QUEUE,
        on_message_callback=_on_message,
        auto_ack=False,
    )
    logger.info(
        "worker consuming from %s with prefetch_count=%s",
        config.PRICING_QUEUE,
        config.PREFETCH_COUNT,
    )
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
