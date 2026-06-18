# TaaSim Mobility Data Engineering

A FastAPI-based ride reservation and demand forecasting API, with supporting infrastructure for Kafka, Cassandra, MinIO, Flink, Spark, and Grafana.

## Quick start

```bash
# Install dependencies
python3 -m pip install -r requirements.txt

# Start the API locally
python -m uvicorn taasim.main:app --host 0.0.0.0 --port 8001

# Reserve a trip
curl -X POST http://localhost:8001/reserve_trip \
  -H "Content-Type: application/json" \
  -d '{"rider_id": 1, "origin_zone": 10, "destination_zone": 20, "call_type": "A"}'

# Get demand forecast
curl -X POST http://localhost:8001/api/demand/forecast \
  -H "Content-Type: application/json" \
  -d '{"zone_id": 1, "datetime": "2026-06-17T12:00:00"}'
```

## Docker Compose

```bash
docker compose up -d
```

Brings up: Kafka, Cassandra, MinIO, Flink, Spark, Grafana, Kafka Connect, and the API.

## Project structure

```
taasim/                      # Python package
├── main.py                  # App factory (create_app + lifespan)
├── config.py                # Pydantic-settings (env-driven config)
├── models.py                # Domain + API schemas (Pydantic)
├── dependencies.py          # Shared FastAPI dependencies (auth, etc.)
├── api/                     # HTTP layer
│   ├── router.py            # Root APIRouter, includes sub-routers
│   ├── trips.py             # POST /reserve_trip, GET /trip_status/{id}
│   ├── forecast.py          # POST /api/demand/forecast
│   └── auth.py              # POST /auth/token
├── services/                # Business logic
│   ├── kafka.py             # Kafka producer/consumer + trip consumer loop
│   └── spark.py             # SparkService (ML model serving)
└── core/                    # Cross-cutting concerns
    └── logging.py           # Logging configuration

scripts/
├── jobs/                    # Spark batch jobs (ETL, KPI analytics)
└── producers/               # Kafka data producers

config/
├── cassandra/               # CQL schema definitions
└── kafka/                   # Kafka Connect connector configs

contracts/                   # Event schemas (JSON Schema)
notebooks/                   # Jupyter notebooks (EDA, training, analysis)
docs/                        # Documentation and reference materials
docker/                      # Dockerfiles (API, Flink)
tests/                       # API tests
docker-compose.yaml          # Local deployment stack
```

## Tech stack

- **API**: FastAPI + Uvicorn (lifespan-managed)
- **Streaming**: Apache Kafka (KRaft mode) + Kafka Connect
- **Storage**: Apache Cassandra + MinIO (S3-compatible)
- **Processing**: Apache Spark (batch ETL + ML) + Apache Flink (stream)
- **Visualization**: Grafana
- **Auth**: JWT (optional, disabled by default)
- **Config**: pydantic-settings v2 (env-driven, `.env` file)
