# TaaSim Technical Report
**Advanced Big Data Capstone Project 2025-2026**
**Team:** Data Engineers (ENSAH)

## 1. Executive Summary
TaaSim is a Transport-as-a-Service data platform designed to resolve urban mobility fragmentation in Casablanca, Morocco's economic capital. Despite significant public investments, the city's transport ecosystem—comprising grand taxis, petits taxis, and public transit—remains disconnected. By capturing real-time GPS telemetry and trip requests, the TaaSim platform orchestrates dynamic matching between passengers and drivers, all while computing proactive demand forecasts using machine learning.

This report details the end-to-end data engineering architecture, outlining the ingestion, stream processing, batch analytics, and machine learning models that power the TaaSim platform. The pipeline was designed to handle high-throughput event streams with low latency, ensuring fault tolerance and scalability.

## 2. Architecture Decisions
### 2.1 The Kappa Architecture Framework
We opted for a **Kappa Architecture** over the traditional Lambda approach. In a Kappa architecture, all data flows through a unified streaming layer, treating batch processing as a special case of stream processing where the stream is bounded. 

- **Single System of Record:** Apache Kafka acts as the sole system of record for all incoming streaming data (GPS streams, Trip Reservations). This allows our Flink pipeline to process live data and replay historical streams seamlessly without managing a separate batch pipeline for the same transformations.
- **Why Kappa?** It reduces codebase duplication. Instead of maintaining one Spark batch job and one Flink streaming job that perform the exact same business logic (e.g., matching a trip or normalizing GPS data), we maintain a single Flink application.

### 2.2 Storage Choices and Justifications
Our storage layer is divided by access patterns and latency requirements:

1. **Apache Kafka (Event Bus):** Provides high-throughput, fault-tolerant ingestion. We configured a 7-day retention policy on raw topics (`raw.gps`, `raw.trips`) to allow for ML feature recomputation and disaster recovery replay without re-downloading source datasets.
2. **MinIO (Data Lake):** An S3-compatible object store. It acts as the persistent, immutable source of truth for offline analytics.
    - **Kafka Connect S3 Sink:** Automatically archives raw events to the `kafka-archive/` bucket.
    - **Curated Zone:** Stores cleaned, partitioned Parquet files processed by Spark.
    - **ML Zone:** Stores the serialized Spark MLlib models for API serving.
3. **Apache Cassandra (Serving Database):** Chosen for its masterless architecture and sub-millisecond write latency. It serves as the operational database for the FastAPI backend and the Grafana dashboard.

## 3. Data Model — Cassandra Schema Design
Cassandra's schema design must be query-driven. Denormalization is not a flaw; it is a feature. We structured our partition keys to explicitly answer the queries issued by the API and Dashboard.

### 3.1 `vehicle_positions` Table
```sql
CREATE TABLE IF NOT EXISTS taasim.vehicle_positions (
  city TEXT,
  zone_id INT,
  event_time BIGINT,
  taxi_id INT,
  latitude DOUBLE,
  longitude DOUBLE,
  speed DOUBLE,
  status TEXT,
  PRIMARY KEY ((city, zone_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
```
- **Partition Key `(city, zone_id)`:** The primary query from Flink Job 3 (Trip Matcher) is *"Give me the most recent available vehicles in Zone X"*. If we partitioned by `taxi_id`, we would have to scatter-gather across the entire cluster to find vehicles in a specific zone. Partitioning by zone ensures all vehicles for that area reside on the same node.
- **Clustering Key `event_time DESC`:** Ensures that the most recent GPS ping is always at the top of the partition, making `LIMIT 1` queries instant.

### 3.2 `trips` Table
```sql
CREATE TABLE IF NOT EXISTS taasim.trips (
  city TEXT,
  date_bucket TEXT,
  created_at BIGINT,
  trip_id TEXT,
  rider_id INT,
  origin_zone INT,
  destination_zone INT,
  call_type TEXT,
  matched_taxi TEXT,
  eta_seconds INT,
  status TEXT,
  PRIMARY KEY ((city, date_bucket), created_at, trip_id)
) WITH CLUSTERING ORDER BY (created_at DESC);
```
- **Partition Key `(city, date_bucket)`:** A single city partition would grow infinitely, eventually exceeding Cassandra's partition size limits (resulting in hotspots and tombstone issues). By introducing a `date_bucket` (e.g., `2026-06-19`), we constrain partition growth and distribute the load evenly across the cluster.

## 4. Stream Processing Pipelines (Apache Flink)
The real-time intelligence of TaaSim is driven by three decoupled Flink jobs.

### 4.1 Flink Job 1: GPS Normalizer & Anonymizer
- **Goal:** Ingest raw GPS pings, validate coordinates, map them to Casablanca's 16 arrondissements, and enforce data privacy.
- **Event-Time Watermarking:** GPS data is inherently noisy and often arrives out-of-order due to network blind spots. We implemented an event-time watermark with a 3-minute allowed lateness threshold.
- **Anonymization:** Raw latitude and longitude coordinates are snapped to the centroid of the matched Casablanca zone. Raw coordinates are never persisted in the database, ensuring rider and driver privacy.

### 4.2 Flink Job 2: Demand Aggregator
- **Goal:** Compute the supply/demand ratio per zone in real-time.
- **Windowing Strategy:** Utilizes a 30-second tumbling window. It aggregates the count of active vehicles and pending trip requests, outputting the exact metrics required for the Grafana Demand Heatmap.

### 4.3 Flink Job 3: Stateful Trip Matcher
- **Goal:** The core dispatch engine.
- **Stateful Processing:** Maintains the state of all available vehicles using Flink's `ValueState` (backed by RocksDB). When a trip request arrives, it queries the state for the nearest vehicle in the exact zone.
- **Fallback Logic:** If no vehicle is found, a Flink Processing-Time Timer is registered for 5 seconds later. If the trip remains unfulfilled, it expands the search radius to adjacent zones.

## 5. Batch Analytics & Machine Learning (Apache Spark)
### 5.1 ETL Processing
We utilized Apache Spark to process historical trip data (Porto proxy dataset and NYC TLC). The batch ETL reads the raw CSV/Parquet files from MinIO, cleans the data, and writes the refined output back to MinIO as partitioned Parquet files (`s3a://curated/trips/`). 

### 5.2 Demand Forecasting (MLlib)
To transform TaaSim from a reactive dispatcher to a proactive platform, we trained a predictive model.
- **Algorithm:** Gradient Boosted Trees (GBT) Regressor.
- **Target:** Number of trip requests in a specific zone 30 minutes in the future.
- **Features:** 
    - *Temporal:* Hour of day, day of week, is_weekend.
    - *Spatial:* Zone ID, population density.
    - *Weather:* Is_raining, temperature buckets (joined from Open-Meteo).
- **Evaluation:** The model was evaluated against a naive 7-day-lag baseline. The GBT model successfully achieved a lower RMSE and MAE, proving its predictive value. The model is serialized to MinIO and served via the FastAPI backend.

## 6. API & Security Layer
- **FastAPI Backend:** A modular REST API handling user requests and serving ML forecasts.
- **JWT Authentication:** Implemented role-based access control (`rider`, `admin`). A `POST /auth/token` endpoint issues JWTs using OAuth2 password flow. The JWT payload encodes the user's role, ensuring riders cannot access administrative endpoints or cluster metrics.

## 7. Non-Functional Requirements & SLAs
During full-system integration testing (running all Kafka producers, Flink Jobs, Spark Jobs, and API load simultaneously), we measured the following Service Level Agreements (SLAs):

| Metric | Target | Actual Measurement | Status |
| :--- | :--- | :--- | :--- |
| **Trip Match Latency** | < 5 seconds P95 | **1.2 seconds** | ✅ Passed |
| **GPS Freshness** | < 15 seconds | **4.5 seconds** | ✅ Passed |
| **Demand Update Frequency**| Every 30 seconds | **30 seconds** (Tumbling Window) | ✅ Passed |
| **ML Forecast API Response**| < 500ms at 20 req/s | **120ms** | ✅ Passed |
| **Spark ETL Execution** | < 5 minutes (1.7M rows) | **2m 14s** | ✅ Passed |

*Resilience Note: Flink checkpointing was verified manually. By killing a TaskManager process, the Flink job seamlessly recovered its state from the MinIO checkpoint directory within 12 seconds, resulting in zero data loss.*

## 8. Challenges & Post-Mortem
1. **OOM Crashes:** During initial integration, the Cassandra container repeatedly crashed due to Out-Of-Memory (OOM) errors. We resolved this by explicitly constraining its heap size (`MAX_HEAP_SIZE=512M`) in the Docker configuration, stabilizing the cluster.
2. **Geospatial Mapping:** Mapping Porto coordinates to Casablanca required significant iterative testing to maintain relative spatial distributions and ensure the visual map looked realistic.
3. **Kafka Connect Networking:** We encountered silent failures where the S3 Sink Connector was running but misconfigured to write to the wrong bucket alias. Re-routing the `topics.dir` resolved the archiving issue.

## 9. Conclusion
The TaaSim platform demonstrates that scalable data engineering is the fundamental requirement for smart city transport solutions. By bridging the gap between disconnected drivers and riders via a low-latency, fault-tolerant data layer, and anticipating demand through machine learning, we have established the groundwork for a modern urban mobility ecosystem in Casablanca.
