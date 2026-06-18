# TaaSim Mobility Data Engineering

A FastAPI-based ride reservation and demand forecasting API, with supporting infrastructure for Kafka, Cassandra, MinIO, Flink, Spark, and Grafana.

## Unified Management

The project features a single entrypoint script (`main.py`) to manage all services and pipelines.

```bash
# ONE SINGLE COMMAND to start services, wait for readiness, and run the pipeline
python main.py run

# Alternatively, manage steps individually:
# Start all services (Infrastructure + API)
python main.py start
...
# Run the data processing pipeline (Data Generation -> ETL -> Analytics)
python main.py pipeline

# Check status
python main.py status

# See logs
python main.py logs

# Stop everything
python main.py stop
```

## Quick start (Local Development)

If you prefer to run only the API locally:
...

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
