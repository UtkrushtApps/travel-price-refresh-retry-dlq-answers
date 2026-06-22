from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import router
from app.rabbitmq import close_publisher, declare_topology


@asynccontextmanager
async def lifespan(application: FastAPI):
    declare_topology()
    yield
    close_publisher()


app = FastAPI(title="TripForge Pricing API", lifespan=lifespan)
app.include_router(router, prefix="/api/v1")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
