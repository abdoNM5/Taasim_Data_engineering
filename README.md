# TaaSim Mobility Data Engineering

This repository contains a FastAPI-based TaaSim API stub for ride reservation and demand forecasting, plus supporting tooling for Kafka, Cassandra, MinIO, and Spark.

## Quick start

1. Install dependencies:
   ```bash
   python3 -m pip install -r requirements.txt
   ```

2. Start the API locally:
   ```bash
   cd /home/amine/Taasim_Data_engineering
   python -m uvicorn taasim.main:app --host 0.0.0.0 --port 8001
   ```

3. Reserve a trip:
   ```bash
   curl -X POST http://localhost:8001/reserve_trip \
     -H "Content-Type: application/json" \
     -d '{"rider_id": 1, "origin_zone": 10, "destination_zone": 20, "call_type": "A"}'
   ```

4. Get demand forecast:
   ```bash
   curl -X POST http://localhost:8001/api/demand/forecast \
     -H "Content-Type: application/json" \
     -d '{"zone_id": 1, "datetime": "2026-06-17T12:00:00"}'
   ```

## Docker Compose

Use `docker-compose.yaml` to bring up Kafka, Cassandra, MinIO, Flink, Spark, and Grafana.

## Project structure

- `src/taasim/` — API package and service helpers
- `api_stub.py` — legacy entrypoint, preserved for compatibility
- `Dockerfile.flink` — custom Flink image build definition
- `docker-compose.yaml` — local deployment stack
