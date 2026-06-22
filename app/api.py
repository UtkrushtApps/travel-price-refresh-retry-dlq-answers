import uuid

from fastapi import APIRouter

from app.rabbitmq import publish_price_refresh
from app.schemas import PriceRefreshAccepted, PriceRefreshRequest

router = APIRouter()


@router.post(
    "/itineraries/{itinerary_id}/refresh-price",
    response_model=PriceRefreshAccepted,
    status_code=202,
)
def refresh_price(itinerary_id: str, body: PriceRefreshRequest) -> PriceRefreshAccepted:
    """Enqueue a price-refresh job and return immediately.

    Supplier calls intentionally do not happen in this request path. The API only
    serializes a job and publishes it to RabbitMQ so callers receive a quick 202.
    """

    job_id = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())
    job = {
        "job_id": job_id,
        "itinerary_id": itinerary_id,
        "supplier_codes": body.supplier_codes,
        "currency": body.currency,
        "force": body.force,
    }
    publish_price_refresh(job, correlation_id=correlation_id)
    return PriceRefreshAccepted(job_id=job_id, correlation_id=correlation_id)
