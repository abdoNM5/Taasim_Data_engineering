from __future__ import annotations

import time

from fastapi import APIRouter, Depends, HTTPException, Request

from ..core.dependencies import verify_token
from ..core.models import TripRequest, TripStatus
from ..services.kafka import send_trip_request

router = APIRouter()


@router.post("/reserve_trip")
def reserve_trip(
    trip: TripRequest,
    request: Request,
    _auth: dict | None = Depends(verify_token),
) -> TripStatus:
    config = request.app.state.config
    producer = request.app.state.producer
    trip_id = str(time.time_ns())
    payload = {
        "trip_id": trip_id,
        "rider_id": trip.rider_id,
        "origin_zone": trip.origin_zone,
        "destination_zone": trip.destination_zone,
        "call_type": trip.call_type,
        "requested_at": int(time.time() * 1000),
    }
    send_trip_request(producer, config.kafka_raw_trips_topic, payload)
    status = TripStatus(
        trip_id=trip_id,
        status="PENDING",
        rider_id=trip.rider_id,
        origin_zone=trip.origin_zone,
        destination_zone=trip.destination_zone,
        message="Trip reserved, finding a driver...",
    )
    request.app.state.trip_statuses[trip_id] = status
    return status


@router.get("/trip_status/{trip_id}")
def get_trip_status(
    trip_id: str,
    request: Request,
    _auth: dict | None = Depends(verify_token),
) -> TripStatus:
    status = request.app.state.trip_statuses.get(trip_id)
    if not status:
        raise HTTPException(status_code=404, detail="Trip not found")
    return status
