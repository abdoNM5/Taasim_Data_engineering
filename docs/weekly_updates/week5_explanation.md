# TaaSim Project Update - Week 5 & Organization

This document summarizes the changes made to the repository to improve organization and fulfill the requirements for **Week 5: Batch ETL & Analytics**.

## 1. Repository Reorganization
The root directory was cleaned and a structured hierarchy was established on the **`amine`** branch:
- **`config/`**: Configuration files for Cassandra and Kafka Connect.
- **`docker/`**: Dockerfiles for the API and Flink.
- **`docs/`**: Centralized documentation and reference materials.
- **`notebooks/`**: All Jupyter notebooks (data cleaning, analysis, training).
- **`scripts/`**: Separated into `jobs/` (Spark batch processing) and `producers/` (Kafka data ingestion).
- **`taasim/`**: Python package — FastAPI backend, Kafka/Spark services, auth, and models.

## 2. Week 5 Task Extraction
Key tasks were extracted from the capstone PDF and documented in [docs/week5.md](docs/week5.md). These include:
- Implementing Spark ETL for historical trip data.
- Computing big data KPIs using Spark SQL.
- Loading analytics results into Cassandra.

## 3. Implementation of Week 5 Jobs
New Spark jobs were implemented with descriptive names, leveraging existing Casablanca map-matched data:
- **`scripts/jobs/casablanca_batch_etl.py`**: Reads raw CSV data, parses polyline coordinates, and computes geospatial indices (H3 cells). Saves cleaned data as Parquet.
- **`scripts/jobs/mobility_kpi_analyzer.py`**: Aggregates curated data to compute hourly trip volume and zone-based demand density. Persists results to the `demand_zones` Cassandra table.

## 4. Enhanced Exploration
- **`notebooks/Week5_Analytics_Exploration.ipynb`**: Created a new notebook for interactive mobility analysis. It includes logic to parse trip polylines and visualize demand intensity on a Casablanca heat map using `folium`.

---
*Work completed on branch:* **`amine`**
