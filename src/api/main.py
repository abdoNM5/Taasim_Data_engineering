from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .core.config import AppConfig
from .core.logging import configure_logging
from .routes import api_router
from .services.kafka import (
    DummyKafkaConsumer,
    DummyKafkaProducer,
    create_consumer,
    create_producer,
    start_trip_consumer,
)
from .services.spark import DummySparkService, SparkService

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    config: AppConfig = app.state.config
    logger.info("Starting TaaSim API in %s mode", config.environment)

    if config.environment == "test":
        app.state.producer = DummyKafkaProducer()
        app.state.consumer = DummyKafkaConsumer()
        app.state.spark_service = DummySparkService(config)
    else:
        app.state.producer = create_producer(config)
        app.state.consumer = create_consumer(config)
        app.state.spark_service = SparkService(config)
        start_trip_consumer(app)

    yield

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


def create_app(config: AppConfig | None = None) -> FastAPI:
    config = config or AppConfig()
    configure_logging(config.log_level)

    app = FastAPI(title=config.app_name, lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(api_router)

    app.state.config = config
    app.state.trip_statuses = {}

    return app


app = create_app()
