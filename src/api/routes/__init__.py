from __future__ import annotations

from fastapi import APIRouter

from .trips import router as trips_router
from .forecast import router as forecast_router
from .auth import router as auth_router

api_router = APIRouter()
api_router.include_router(trips_router)
api_router.include_router(forecast_router)
api_router.include_router(auth_router)


@api_router.get("/")
def read_root() -> dict[str, str]:
    return {"message": "Welcome to TaaSim API. Use /reserve_trip and /trip_status to interact."}
