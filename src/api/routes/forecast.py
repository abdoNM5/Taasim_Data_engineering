from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request

from ..core.dependencies import verify_token
from ..core.models import ForecastRequest, ForecastResponse

router = APIRouter()


@router.post("/api/demand/forecast", response_model=ForecastResponse)
def get_demand_forecast(
    forecast: ForecastRequest,
    request: Request,
    _auth: dict | None = Depends(verify_token),
) -> ForecastResponse:
    try:
        dt = datetime.fromisoformat(forecast.datetime)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid datetime format. Use YYYY-MM-DDThh:mm:ss")
    predicted_demand = request.app.state.spark_service.predict_demand(forecast.zone_id, dt)
    return ForecastResponse(zone_id=forecast.zone_id, datetime=forecast.datetime, predicted_demand=predicted_demand)
