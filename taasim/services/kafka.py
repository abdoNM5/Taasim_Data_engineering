from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

from ..core.config import AppConfig
from ..core.models import TripStatus

logger = logging.getLogger(__name__)


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


def _import_kafka() -> tuple[Any, Any, Any]:
    try:
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.errors import KafkaError
        return KafkaProducer, KafkaConsumer, KafkaError
    except ImportError as exc:
        raise RuntimeError("kafka-python is required to use Kafka features") from exc


def create_producer(config: AppConfig) -> Any:
    KafkaProducer, _, _ = _import_kafka()
    return KafkaProducer(
        bootstrap_servers=config.kafka_server_list,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8") if key is not None else None,
        retries=5,
        linger_ms=10,
    )


def create_consumer(config: AppConfig) -> Any:
    _, KafkaConsumer, _ = _import_kafka()
    return KafkaConsumer(
        config.kafka_processed_trips_topic,
        bootstrap_servers=config.kafka_server_list,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=config.kafka_api_group_id,
        consumer_timeout_ms=1000,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )


def send_trip_request(producer: Any, topic: str, payload: dict) -> None:
    try:
        producer.send(topic, payload)
        producer.flush()
        logger.info("Published trip request to Kafka topic %s", topic)
    except Exception as exc:
        logger.exception("Unable to publish trip request: %s", exc)
        raise


def consume_processed_trips(app: "FastAPI") -> None:  # noqa: F821
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


def start_trip_consumer(app: "FastAPI") -> None:  # noqa: F821
    thread = threading.Thread(target=consume_processed_trips, args=(app,), daemon=True)
    thread.start()
