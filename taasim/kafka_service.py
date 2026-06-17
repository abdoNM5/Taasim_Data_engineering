import json
import logging
from typing import Any

from .config import AppConfig

logger = logging.getLogger(__name__)


def _import_kafka() -> tuple[Any, Any, Any]:
    try:
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.errors import KafkaError
        return KafkaProducer, KafkaConsumer, KafkaError
    except ImportError as exc:
        raise RuntimeError("kafka-python is required to use Kafka features") from exc


def create_kafka_producer(config: AppConfig) -> Any:
    KafkaProducer, _, _ = _import_kafka()
    return KafkaProducer(
        bootstrap_servers=config.kafka_server_list,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8") if key is not None else None,
        retries=5,
        linger_ms=10,
    )


def create_kafka_consumer(config: AppConfig) -> Any:
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
