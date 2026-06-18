from __future__ import annotations

import os
import logging
from typing import Any
from datetime import datetime

from ..core.config import AppConfig

logger = logging.getLogger(__name__)


def _import_spark() -> tuple[Any, Any, Any]:
    try:
        from pyspark.sql import SparkSession, Row
        from pyspark.ml import PipelineModel
        return SparkSession, Row, PipelineModel
    except ImportError as exc:
        raise RuntimeError("pyspark is required to use SparkService") from exc


class SparkService:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._SparkSession, self._Row, self._PipelineModel = _import_spark()
        self.spark = self._create_spark_session()
        self.model = self._load_model()

    def _create_spark_session(self) -> Any:
        logger.info("Connecting to Spark master at %s", self.config.spark_master_url)

        builder = (
            self._SparkSession.builder
            .appName(self.config.spark_app_name)
            .master(self.config.spark_master_url)          # ← connect to YOUR cluster
            .config("spark.submit.deployMode", "client")
            # S3A / MinIO
            .config("spark.hadoop.fs.s3a.endpoint",          self.config.spark_s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key",        self.config.spark_s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key",        self.config.spark_s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", str(self.config.spark_s3_path_style_access).lower())
            .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # S3A jars
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            )
            # Don't need a full local Spark UI
            .config("spark.ui.enabled", "false")
        )

        return builder.getOrCreate()

    def _load_model(self) -> Any:
        logger.info("Loading ML model from %s", self.config.spark_model_path)
        return self._PipelineModel.load(self.config.spark_model_path)

    def predict_demand(self, zone_id: int, dt: datetime) -> float:
        row = self._Row(
            zone_id=float(zone_id),
            hour_of_day=float(dt.hour),
            day_of_week=float(dt.weekday()),
            is_weekend=1.0 if dt.weekday() >= 5 else 0.0,
            is_friday=1.0 if dt.weekday() == 4 else 0.0,
            is_raining=0.0,
            demand_lag_1d=0.0,
            demand_lag_7d=0.0,
            rolling_7d_mean=0.0,
        )
        df = self.spark.createDataFrame([row])
        prediction = self.model.transform(df).select("prediction").collect()[0][0]
        return max(0.0, float(prediction))

    def stop(self) -> None:
        try:
            self.spark.stop()
        except Exception:
            logger.exception("Error stopping Spark session")


class DummySparkService:
    def __init__(self, config: AppConfig) -> None:
        logger.info("Initializing DummySparkService")

    def predict_demand(self, zone_id: int, dt: datetime) -> float:
        return 0.0

    def stop(self) -> None:
        pass