from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Literal


class TripRequest(BaseModel):
    rider_id: int
    origin_zone: int
    destination_zone: int
    call_type: Literal["A", "B"] = Field("A", description="Trip call type")


class ForecastRequest(BaseModel):
    zone_id: int
    datetime: str


class TripStatus(BaseModel):
    trip_id: str
    status: str
    rider_id: int | None = None
    origin_zone: int | None = None
    destination_zone: int | None = None
    message: str | None = None


class ForecastResponse(BaseModel):
    zone_id: int
    datetime: str
    predicted_demand: float
