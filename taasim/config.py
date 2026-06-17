from __future__ import annotations

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List


class AppConfig(BaseSettings):
    app_name: str = Field("TaaSim API", env="APP_NAME")
    environment: str = Field("development", env="APP_ENV")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    kafka_bootstrap_servers: str = Field("kafka:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    kafka_raw_trips_topic: str = Field("raw.trips", env="KAFKA_RAW_TRIPS_TOPIC")
    kafka_processed_trips_topic: str = Field("processed.trips", env="KAFKA_PROCESSED_TRIPS_TOPIC")
    kafka_api_group_id: str = Field("api-stub-consumer", env="KAFKA_API_GROUP_ID")

    minio_endpoint: str = Field("http://minio:9000", env="MINIO_ENDPOINT")
    minio_access_key: str = Field("admin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field("password123", env="MINIO_SECRET_KEY")

    spark_app_name: str = Field("TaaSim-API", env="SPARK_APP_NAME")
    spark_model_path: str = Field("s3a://ml/models/demand_v1/", env="SPARK_MODEL_PATH")
    spark_s3_endpoint: str = Field("http://minio:9000", env="SPARK_S3_ENDPOINT")
    spark_s3_access_key: str = Field("admin", env="SPARK_S3_ACCESS_KEY")
    spark_s3_secret_key: str = Field("password123", env="SPARK_S3_SECRET_KEY")
    spark_s3_path_style_access: bool = Field(True, env="SPARK_S3_PATH_STYLE_ACCESS")

    api_host: str = Field("0.0.0.0", env="API_HOST")
    api_port: int = Field(8001, env="API_PORT")

    jwt_secret: str = Field("change-me-to-a-strong-secret", env="JWT_SECRET")
    auth_enabled: bool = Field(False, env="AUTH_ENABLED")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def kafka_server_list(self) -> List[str]:
        return [server.strip() for server in self.kafka_bootstrap_servers.split(",") if server.strip()]
