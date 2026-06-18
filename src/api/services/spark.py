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
        self._spark = None
        self._model = None

    def _get_spark(self) -> Any:
        """Lazy-create a local Spark session on first use.

        We use local[*] mode because the API container has the S3A JARs
        needed to read from MinIO, while the remote Spark cluster workers
        do not.  Model loading and single-row prediction are lightweight
        enough to run in-process.
        """
        if self._spark is None:
            logger.info("Creating local Spark session for model serving")
            try:
                self._spark = (
                    self._SparkSession.builder
                    .appName(self.config.spark_app_name)
                    .master("local[*]")
                    .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.hadoop.fs.s3a.endpoint",          self.config.spark_s3_endpoint)
                    .config("spark.hadoop.fs.s3a.access.key",        self.config.spark_s3_access_key)
                    .config("spark.hadoop.fs.s3a.secret.key",        self.config.spark_s3_secret_key)
                    .config("spark.hadoop.fs.s3a.path.style.access", "true")
                    .config("spark.ui.enabled", "false")
                    .config("spark.driver.memory", "512m")
                    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
                    .getOrCreate()
                )
                logger.info("Local Spark session created successfully")
            except Exception:
                logger.exception("Failed to create Spark session")
        return self._spark

    def _get_model(self) -> Any:
        """Lazy-load the ML model on first use. Retries on subsequent calls if it fails."""
        if self._model is not None:
            return self._model
        spark = self._get_spark()
        if spark is None:
            logger.warning("Spark session unavailable — skipping model load")
            return None
        logger.info("Loading ML model from %s", self.config.spark_model_path)
        try:
            self._model = self._PipelineModel.load(self.config.spark_model_path)
            logger.info("ML model loaded successfully")
        except Exception:
            logger.exception(
                "Could not load ML model from %s — forecast endpoint will return fallback values",
                self.config.spark_model_path,
            )
        return self._model

    def predict_demand(self, zone_id: int, dt: datetime) -> float:
        spark = self._get_spark()
        model = self._get_model()
        if spark is None or model is None:
            logger.warning("Spark/model not available — returning fallback prediction 0.0")
            return 0.0

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
        df = spark.createDataFrame([row])
        prediction = model.transform(df).select("prediction").collect()[0][0]
        return max(0.0, float(prediction))

    def stop(self) -> None:
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception:
                logger.exception("Error stopping Spark session")


class DummySparkService:
    def __init__(self, config: AppConfig) -> None:
        logger.info("Initializing DummySparkService")

    def predict_demand(self, zone_id: int, dt: datetime) -> float:
        return 0.0

    def stop(self) -> None:
        pass