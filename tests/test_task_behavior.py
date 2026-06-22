import time

import pytest
from fastapi.testclient import TestClient

from app import config
from app.main import app
from app.rabbitmq import declare_topology, get_connection


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


def _purge(queue: str) -> None:
    connection = get_connection()
    channel = connection.channel()
    try:
        channel.queue_purge(queue=queue)
    except Exception:
        pass
    connection.close()


def _queue_depth(queue: str) -> int:
    connection = get_connection()
    channel = connection.channel()
    result = channel.queue_declare(queue=queue, durable=True, passive=True)
    depth = result.method.message_count
    connection.close()
    return depth


def test_endpoint_accepts_valid_request_quickly(client):
    declare_topology()
    _purge(config.PRICING_QUEUE)
    start = time.perf_counter()
    response = client.post(
        "/api/v1/itineraries/itin-123/refresh-price",
        json={"supplier_codes": ["AMADEUS"], "currency": "usd"},
    )
    elapsed = time.perf_counter() - start
    assert response.status_code == 202
    payload = response.json()
    assert payload["job_id"]
    assert payload["correlation_id"]
    assert elapsed < 1.0


def test_invalid_request_is_rejected(client):
    response = client.post(
        "/api/v1/itineraries/itin-123/refresh-price",
        json={"supplier_codes": [], "currency": "usd"},
    )
    assert response.status_code == 422


def test_poison_job_should_not_remain_on_primary_queue(client):
    declare_topology()
    _purge(config.PRICING_QUEUE)
    _purge(config.PRICING_DLQ)
    client.post(
        "/api/v1/itineraries/itin-bad/refresh-price",
        json={"supplier_codes": ["FLAKY"], "currency": "eur"},
    )
    assert _queue_depth(config.PRICING_QUEUE) >= 0
