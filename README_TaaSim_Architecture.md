# TaaSim: Real-Time Urban Mobility Data Pipeline

This README explains the end-to-end architecture of the TaaSim platform, focusing on how the streaming jobs, databases, and storage layers "talk" to each other to power a real-time transport-as-a-service application.

## 🏗️ High-Level Architecture Overview

TaaSim uses a **Kappa Architecture**. This means all live processing is handled by streaming pipelines (Apache Flink), while historical analysis and Machine Learning are handled by batch jobs (Apache Spark) operating on an object store (MinIO). Apache Kafka sits in the middle as the central nervous system.

Here is the step-by-step flow of data:

### 1. Data Sources (The Simulators)
Since we don't have live taxis in Casablanca, Python scripts act as simulators:
* **`ProducerGps.py`**: Simulates taxis driving around. It sends raw GPS coordinates to the Kafka topic **`raw.gps`**.
* **`ProducerTrips.py`**: Simulates citizens requesting rides via an app. It sends ride requests to the Kafka topic **`raw.trips`**.

### 2. The Streaming Engine (Apache Flink)
Flink consumes the raw data from Kafka in real-time, processes it, and writes the results to both **Cassandra** (for fast API/Dashboard queries) and **Kafka** (for other jobs to consume).

* **Job 1 (GPS Normalizer)**: 
  * Reads `raw.gps`.
  * Snaps the GPS coordinates to Casablanca zones.
  * Writes the cleaned data to Kafka **`processed.gps`** and Cassandra **`vehicle_positions`**.
* **Job 2 (Demand Aggregator)**:
  * Reads `processed.gps` (to know where taxis are) and `raw.trips` (to know where riders are).
  * Uses a 30-second window to count supply and demand per zone.
  * Writes the ratios to Kafka **`processed.demand`** and Cassandra **`demand_zones`**.
* **Job 3 (Trip Matcher)**:
  * Reads `processed.gps` and `raw.trips`.
  * Acts as the dispatcher. If a trip is requested in Zone 1, it looks for an available taxi in Zone 1.
  * Writes the match results (or UNFULFILLED if no taxi is found) to Kafka **`processed.trips`** and Cassandra **`trips`**.

---

## 🗄️ The Serving Layer (Apache Cassandra)

Cassandra is a NoSQL database designed for insanely fast reads and writes. TaaSim uses it to serve live data to the API and Grafana dashboards.

### `taasim.vehicle_positions`
* **What it stores**: The exact, real-time location and speed of every taxi.
* **How it's used**: A live map in Grafana querying "Show me all AVAILABLE taxis in Zone 1".
* **Partition Key**: `(city, zone_id)` — This ensures all taxis in the same zone are stored together on the same server, making queries ultra-fast.

### `taasim.demand_zones`
* **What it stores**: The 30-second aggregated counts (`active_vehicles`, `pending_requests`, and `ratio`).
* **How it's used**: The live heatmap in Grafana showing which zones are currently surging with demand.

### `taasim.trips`
* **What it stores**: The history of every citizen's ride request and whether it was successfully matched to a taxi (`matched_taxi`).
* **How it's used**: Showing a rider their trip history or calculating daily revenue.

---

## 🪣 The Data Lake (MinIO)

While Cassandra handles the *live* data (the present), **MinIO handles the *historical* data (the past).** MinIO is an S3-compatible object storage server.

How does data get to MinIO?
1. **Kafka Connect (S3 Sink)**: As data flows through Kafka, a background connector constantly dumps a copy of every single raw event into MinIO's `raw/kafka-archive/` bucket. This means you never lose a single GPS ping.
2. **Flink Checkpoints**: Every 60 seconds, Flink saves its internal memory (state) to MinIO. If the server crashes, Flink reads from MinIO and resumes exactly where it left off without losing data.

How is MinIO used?
* **Spark ETL**: Every night, Apache Spark wakes up, reads the raw CSVs and archived Kafka data from MinIO, cleans it, and saves highly optimized Parquet files into the `curated/` bucket.
* **Machine Learning**: Spark MLlib reads those curated Parquet files to train a Gradient Boosted Tree model that predicts future taxi demand. The trained model artifact is saved back to MinIO (`ml/models/`) so the FastAPI server can load it and serve predictions.

## 🔄 Summary of How They "Talk"
1. **Simulators** talk to **Kafka** (produce).
2. **Flink** talks to **Kafka** (consume/produce), **Cassandra** (write live data), and **MinIO** (save crash backups).
3. **Kafka Connect** talks to **Kafka** and **MinIO** (archive everything forever).
4. **Spark** talks only to **MinIO** (read history, write analytics and AI models).
5. **Grafana / FastAPI** talks to **Cassandra** (read live data) and **MinIO** (load AI models).
