from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import List


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
    spark_master_url: str = Field("spark://spark-master:7077", alias="SPARK_MASTER_URL")
    app_name: str = Field("TaaSim API", alias="APP_NAME")
    environment: str = Field("development", alias="APP_ENV")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    kafka_bootstrap_servers: str = Field("localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_raw_trips_topic: str = Field("raw.trips", alias="KAFKA_RAW_TRIPS_TOPIC")
    kafka_processed_trips_topic: str = Field("processed.trips", alias="KAFKA_PROCESSED_TRIPS_TOPIC")
    kafka_api_group_id: str = Field("api-stub-consumer", alias="KAFKA_API_GROUP_ID")

    minio_endpoint: str = Field("http://minio:9000", alias="MINIO_ENDPOINT")
    minio_access_key: str = Field("admin", alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field("password123", alias="MINIO_SECRET_KEY")

    spark_app_name: str = Field("TaaSim-API", alias="SPARK_APP_NAME")
    spark_model_path: str = Field("s3a://ml/models/demand_v1/", alias="SPARK_MODEL_PATH")
    spark_s3_endpoint: str = Field("http://minio:9000", alias="SPARK_S3_ENDPOINT")
    spark_s3_access_key: str = Field("admin", alias="SPARK_S3_ACCESS_KEY")
    spark_s3_secret_key: str = Field("password123", alias="SPARK_S3_SECRET_KEY")
    spark_s3_path_style_access: bool = Field(True, alias="SPARK_S3_PATH_STYLE_ACCESS")

    api_host: str = Field("0.0.0.0", alias="API_HOST")
    api_port: int = Field(8001, alias="API_PORT")

    jwt_secret: str = Field("change-me-to-a-strong-secret", alias="JWT_SECRET")
    auth_enabled: bool = Field(False, alias="AUTH_ENABLED")

    @property
    def kafka_server_list(self) -> List[str]:
        return [server.strip() for server in self.kafka_bootstrap_servers.split(",") if server.strip()]
