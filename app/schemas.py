from typing import List

from pydantic import BaseModel, Field, field_validator


class PriceRefreshRequest(BaseModel):
    supplier_codes: List[str] = Field(..., min_length=1)
    currency: str = Field(..., min_length=3, max_length=3)
    force: bool = False

    @field_validator("currency")
    @classmethod
    def upper_currency(cls, value: str) -> str:
        return value.upper()


class PriceRefreshAccepted(BaseModel):
    job_id: str
    correlation_id: str
    status: str = "accepted"
