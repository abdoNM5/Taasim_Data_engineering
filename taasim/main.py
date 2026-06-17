from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware

from .config import AppConfig
from .kafka_service import create_kafka_consumer, create_kafka_producer, send_trip_request
from .logging_config import configure_logging
from .models import ForecastRequest, ForecastResponse, TripRequest, TripStatus
from .spark_service import DummySparkService, SparkService
from .auth import token_endpoint, verify_token

from fastapi.security import OAuth2PasswordRequestForm

logger = logging.getLogger(__name__)
router = APIRouter()


class DummyKafkaProducer:
    def send(self, topic: str, value: dict[str, Any]) -> None:
        logger.debug("DummyProducer.send called for topic %s", topic)

    def flush(self) -> None:
        logger.debug("DummyProducer.flush called")

    def close(self) -> None:
        logger.debug("DummyProducer.close called")


class DummyKafkaConsumer:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("DummyKafkaConsumer initialized")

    def __iter__(self) -> Any:
        return iter(())

    def close(self) -> None:
        logger.debug("DummyKafkaConsumer.close called")


def consume_processed_trips(app: FastAPI) -> None:
    consumer = app.state.consumer
    while True:
        try:
            for message in consumer:
                trip_data = message.value
                trip_id = trip_data.get("trip_id")
                if trip_id:
                    app.state.trip_statuses[trip_id] = TripStatus(
                        trip_id=trip_id,
                        status=trip_data.get("status", "UNKNOWN"),
                        rider_id=trip_data.get("rider_id"),
                        origin_zone=trip_data.get("origin_zone"),
                        destination_zone=trip_data.get("destination_zone"),
                        message=trip_data.get("message", ""),
                    )
        except Exception as exc:
            logger.exception("Kafka consumer failed: %s", exc)
            time.sleep(3)


def create_app(config: AppConfig | None = None) -> FastAPI:
    config = config or AppConfig()
    configure_logging(config.log_level)

    app = FastAPI(title=config.app_name)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router)

    app.state.config = config
    app.state.trip_statuses = {}

    @app.on_event("startup")
    def on_startup() -> None:
        logger.info("Starting TaaSim API in %s mode", config.environment)
        if config.environment == "test":
            app.state.producer = DummyKafkaProducer()
            app.state.consumer = DummyKafkaConsumer()
            app.state.spark_service = DummySparkService(config)
        else:
            app.state.producer = create_kafka_producer(config)
            app.state.consumer = create_kafka_consumer(config)
            app.state.spark_service = SparkService(config)
            thread = threading.Thread(target=consume_processed_trips, args=(app,), daemon=True)
            thread.start()

    @app.on_event("shutdown")
    def on_shutdown() -> None:
        logger.info("Shutting down TaaSim API")
        if hasattr(app.state, "producer"):
            try:
                app.state.producer.close()
            except Exception:
                logger.exception("Error closing Kafka producer")
        if hasattr(app.state, "consumer"):
            try:
                app.state.consumer.close()
            except Exception:
                logger.exception("Error closing Kafka consumer")
        if hasattr(app.state, "spark_service"):
            try:
                app.state.spark_service.stop()
            except Exception:
                logger.exception("Error stopping Spark service")

    return app


@router.get("/")
def read_root() -> dict[str, str]:
    return {"message": "Welcome to TaaSim API. Use /reserve_trip and /trip_status to interact."}


@router.post("/reserve_trip")
def reserve_trip(request: TripRequest, request_: Request, _auth: dict | None = Depends(verify_token)) -> TripStatus:
    config = request_.app.state.config
    producer = request_.app.state.producer
    trip_id = str(time.time_ns())
    payload = {
        "trip_id": trip_id,
        "rider_id": request.rider_id,
        "origin_zone": request.origin_zone,
        "destination_zone": request.destination_zone,
        "call_type": request.call_type,
        "requested_at": int(time.time() * 1000),
    }
    send_trip_request(producer, config.kafka_raw_trips_topic, payload)
    status = TripStatus(
        trip_id=trip_id,
        status="PENDING",
        rider_id=request.rider_id,
        origin_zone=request.origin_zone,
        destination_zone=request.destination_zone,
        message="Trip reserved, finding a driver...",
    )
    request_.app.state.trip_statuses[trip_id] = status
    return status


@router.get("/trip_status/{trip_id}")
def get_trip_status(trip_id: str, request_: Request, _auth: dict | None = Depends(verify_token)) -> TripStatus:
    status = request_.app.state.trip_statuses.get(trip_id)
    if not status:
        raise HTTPException(status_code=404, detail="Trip not found")
    return status


@router.post("/api/demand/forecast", response_model=ForecastResponse)
def get_demand_forecast(request: ForecastRequest, request_: Request, _auth: dict | None = Depends(verify_token)) -> ForecastResponse:
    try:
        dt = datetime.fromisoformat(request.datetime)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid datetime format. Use YYYY-MM-DDThh:mm:ss")
    predicted_demand = request_.app.state.spark_service.predict_demand(request.zone_id, dt)
    return ForecastResponse(zone_id=request.zone_id, datetime=request.datetime, predicted_demand=predicted_demand)


app = create_app()




# Auth token endpoint (OAuth2 compatible form)
@router.post("/auth/token")
async def auth_token(form_data: OAuth2PasswordRequestForm = Depends()) -> dict:
    return await token_endpoint(form_data, None)
