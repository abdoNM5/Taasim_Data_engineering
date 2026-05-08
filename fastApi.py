"""
TaaSim — FastAPI Service
Week 6: POST /api/demand/forecast endpoint

Run:  uvicorn main:app --reload --port 8000
Auth: POST /auth/token  →  Bearer token  →  use on /api/demand/forecast
"""

import os
import json
import math
from datetime import datetime, timedelta, date
from typing import Optional

import boto3
from botocore.client import Config
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from pydantic import BaseModel

# ── PySpark (loaded once at startup) ─────────────────────────────────────────
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DoubleType, TimestampType, DateType
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS     = os.getenv("MINIO_ACCESS",     "admin")
MINIO_SECRET     = os.getenv("MINIO_SECRET",     "password123")
ML_MODEL_PATH    = os.getenv("ML_MODEL_PATH",    "s3a://ml/models/demand_v1/")

CASSANDRA_HOST   = os.getenv("CASSANDRA_HOST",   "localhost")

# JWT config
SECRET_KEY   = os.getenv("JWT_SECRET", "taasim-super-secret-2026")
ALGORITHM    = "HS256"
TOKEN_EXPIRE = 60  # minutes

# Fake user store (replace with DB in production)
FAKE_USERS = {
    "admin":  {"username": "admin",  "role": "admin",  "password": "admin123"},
    "rider1": {"username": "rider1", "role": "rider",  "password": "rider123"},
}

# Feature column order MUST match training notebook
FEATURE_COLS = [
    "zone_id", "hour_of_day", "day_of_week",
    "is_weekend", "is_friday", "is_raining",
    "demand_lag_1d", "demand_lag_7d", "rolling_7d_mean",
]

# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION (singleton, created once)
# ─────────────────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    # On Windows match your Week-5 Java path; on Docker/Linux this is ignored
    java_home = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
    if os.path.exists(java_home):
        os.environ["JAVA_HOME"] = java_home
    os.environ["SPARK_HOME"] = pyspark.__path__[0]
    if "PYSPARK_SUBMIT_ARGS" in os.environ:
        del os.environ["PYSPARK_SUBMIT_ARGS"]

    return (
        SparkSession.builder
        .appName("TaaSim-API")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.367"
        )
        .config("spark.hadoop.fs.s3a.endpoint",              MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",            MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",            MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .config(
            "spark.driver.extraJavaOptions",
            "-Dio.netty.tryReflectionSetAccessible=true "
            "--add-opens=java.base/java.nio=ALL-UNNAMED"
        )
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# APPLICATION LIFESPAN (load Spark + model once at startup)
# ─────────────────────────────────────────────────────────────────────────────

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load heavy resources once at startup, release on shutdown."""
    print("⏳ Starting Spark and loading ML model...")
    app.state.spark = build_spark()
    app.state.spark.sparkContext.setLogLevel("ERROR")
    app.state.model = PipelineModel.load(ML_MODEL_PATH)
    print("✅ Spark + GBT model ready.")
    yield
    app.state.spark.stop()
    print("Spark stopped.")


app = FastAPI(
    title="TaaSim API",
    description="Urban Mobility Platform — Casablanca",
    version="1.0.0",
    lifespan=lifespan,
)

# ─────────────────────────────────────────────────────────────────────────────
# JWT AUTH
# ─────────────────────────────────────────────────────────────────────────────

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")


def create_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE)
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    """Decode JWT and return user dict. Raises 401 on invalid token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        if not username:
            raise HTTPException(status_code=401, detail="Invalid token.")
        return {"username": username, "role": role}
    except JWTError:
        raise HTTPException(status_code=401, detail="Could not validate token.")


def require_admin(user: dict = Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin role required.")
    return user


# ─────────────────────────────────────────────────────────────────────────────
# PYDANTIC SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────

class ForecastRequest(BaseModel):
    zone_id: int                  # 1–16 (Casablanca arrondissement)
    datetime: datetime            # Target slot datetime (UTC)
    demand_lag_1d: float = 0.0    # Optional: known demand 1 day before
    demand_lag_7d: float = 0.0    # Optional: known demand 7 days before
    rolling_7d_mean: float = 0.0  # Optional: rolling mean
    is_raining: int = 0           # Optional: 1 if raining


class ForecastResponse(BaseModel):
    zone_id: int
    slot_datetime: datetime
    predicted_demand: float
    model_version: str = "demand_v1"


class TokenResponse(BaseModel):
    access_token: str
    token_type: str


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: build single-row Spark DataFrame for inference
# ─────────────────────────────────────────────────────────────────────────────

def _build_input_df(spark: SparkSession, req: ForecastRequest):
    """
    Create a one-row Spark DataFrame that matches the training schema.
    The model pipeline (VectorAssembler → StandardScaler → GBT)
    expects all FEATURE_COLS to be present as double columns.
    """
    dt = req.datetime

    # day_of_week: Spark uses Sun=1, Sat=7
    # Python weekday() is Mon=0 → we convert
    py_dow = dt.weekday()            # Mon=0 … Sun=6
    spark_dow = (py_dow + 2) % 7    # Mon=2 … Sun=1
    if spark_dow == 0:
        spark_dow = 7

    row = [(
        float(req.zone_id),
        float(dt.hour),
        float(spark_dow),
        float(1 if spark_dow in [1, 7] else 0),   # is_weekend
        float(1 if spark_dow == 6 else 0),         # is_friday (Fri=6 in Spark)
        float(req.is_raining),
        float(req.demand_lag_1d),
        float(req.demand_lag_7d),
        float(req.rolling_7d_mean),
        0.0,  # demand placeholder (target — ignored at inference)
    )]

    schema = StructType([
        StructField("zone_id",         DoubleType()),
        StructField("hour_of_day",     DoubleType()),
        StructField("day_of_week",     DoubleType()),
        StructField("is_weekend",      DoubleType()),
        StructField("is_friday",       DoubleType()),
        StructField("is_raining",      DoubleType()),
        StructField("demand_lag_1d",   DoubleType()),
        StructField("demand_lag_7d",   DoubleType()),
        StructField("rolling_7d_mean", DoubleType()),
        StructField("demand",          DoubleType()),
    ])

    return spark.createDataFrame(row, schema=schema)


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/auth/token", response_model=TokenResponse, tags=["Auth"])
def login(form: OAuth2PasswordRequestForm = Depends()):
    """
    Get a JWT token.

    Test credentials:
    - admin / admin123  (role: admin)
    - rider1 / rider123 (role: rider)
    """
    user = FAKE_USERS.get(form.username)
    if not user or user["password"] != form.password:
        raise HTTPException(status_code=401, detail="Incorrect credentials.")
    token = create_token({"sub": user["username"], "role": user["role"]})
    return {"access_token": token, "token_type": "bearer"}


@app.post(
    "/api/demand/forecast",
    response_model=ForecastResponse,
    tags=["ML"],
    summary="Predict trip demand for a zone+time slot",
)
def forecast_demand(
    req: ForecastRequest,
    user: dict = Depends(get_current_user),   # any authenticated user
):
    """
    Predict how many trip requests will occur in `zone_id`
    during the 30-minute slot containing `datetime`.

    - **zone_id**: Casablanca arrondissement (1–16)
    - **datetime**: Target UTC datetime (result is for the 30-min slot it falls in)
    - **demand_lag_1d / demand_lag_7d / rolling_7d_mean**: historical hints (optional)

    Returns `predicted_demand` (float ≥ 0).
    """
    if req.zone_id < 1 or req.zone_id > 16:
        raise HTTPException(status_code=422, detail="zone_id must be 1–16.")

    spark = app.state.spark
    model = app.state.model

    # Snap datetime to 30-min slot boundary
    dt = req.datetime.replace(second=0, microsecond=0)
    slot_minute = (dt.minute // 30) * 30
    slot_dt = dt.replace(minute=slot_minute)

    # Build input and run inference
    input_df = _build_input_df(spark, ForecastRequest(
        zone_id=req.zone_id,
        datetime=slot_dt,
        demand_lag_1d=req.demand_lag_1d,
        demand_lag_7d=req.demand_lag_7d,
        rolling_7d_mean=req.rolling_7d_mean,
        is_raining=req.is_raining,
    ))

    result = model.transform(input_df).collect()[0]
    predicted = max(0.0, round(result["prediction"], 2))

    return ForecastResponse(
        zone_id=req.zone_id,
        slot_datetime=slot_dt,
        predicted_demand=predicted,
    )


@app.get(
    "/api/demand/forecast/bulk",
    tags=["ML"],
    summary="Forecast next 48 slots for all zones (admin only)",
)
def forecast_bulk(user: dict = Depends(require_admin)):
    """
    Returns demand forecast for all 16 zones × next 48 thirty-minute slots.
    Admin role required.
    """
    spark = app.state.spark
    model = app.state.model

    now  = datetime.utcnow().replace(second=0, microsecond=0)
    slot = now.replace(minute=(now.minute // 30) * 30)
    slots = [slot + timedelta(minutes=30 * i) for i in range(48)]
    zones = list(range(1, 17))

    rows = []
    for s in slots:
        py_dow = s.weekday()
        spark_dow = (py_dow + 2) % 7 or 7
        for z in zones:
            rows.append((
                float(z), float(s.hour), float(spark_dow),
                float(1 if spark_dow in [1, 7] else 0),
                float(1 if spark_dow == 6 else 0),
                0.0, 0.0, 0.0, 0.0, 0.0
            ))

    schema = StructType([
        StructField(c, DoubleType()) for c in FEATURE_COLS + ["demand"]
    ])
    input_df = spark.createDataFrame(rows, schema=schema)
    preds = model.transform(input_df).collect()

    results = []
    for row, (s, z) in zip(preds, [(s, z) for s in slots for z in zones]):
        results.append({
            "zone_id": z,
            "slot_datetime": s.isoformat(),
            "predicted_demand": max(0.0, round(row["prediction"], 2)),
        })

    return {"count": len(results), "forecasts": results}


@app.get("/health", tags=["System"])
def health():
    """Health check — verify Spark and model are loaded."""
    model_ok = hasattr(app.state, "model") and app.state.model is not None
    return {"status": "ok", "model_loaded": model_ok}