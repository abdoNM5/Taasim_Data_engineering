# TaaSim Casablanca - Urban Mobility Data Platform

National School of Applied Sciences - Al Hoceima (ENSAH)  
Advanced Big Data Capstone Project (2025-2026)

## Project Overview

Casablanca is a metropolis of 4 million people with a deeply fragmented urban mobility network. TaaSim is a Transport-as-a-Service platform that treats urban mobility as a data engineering problem.

This repository contains the code for a Kappa-Architecture Big Data pipeline designed to ingest live GPS streams, process citizen trip reservations in real-time, and apply machine learning to forecast demand surges across Casablanca's 16 arrondissements.

## Technology Stack

- Streaming Engine: Apache Flink
- Message Broker: Apache Kafka (KRaft mode)
- Data Lake: MinIO (S3-Compatible Object Storage)
- Batch Processing & ML: Apache Spark (PySpark & MLlib)
- Serving Database: Apache Cassandra
- API & Visualization: FastAPI + Grafana

## Week 1 Accomplishments: Foundation & Simulation Layer

Due to the lack of open-source transit data for Casablanca, this project utilizes the Porto Taxi Trajectories (ECML 2015) dataset and the NYC TLC Trip Records as proxies. Week 1 focused on infrastructure setup, geospatial data engineering, and building a robust, enterprise-grade real-time simulation layer.

### 1. Infrastructure & Storage Initialization

- Successfully provisioned the complete local cluster using Docker Compose.
- Configured Kafka with dual listeners:
	- Internal `PLAINTEXT:9092`
	- External `EXTERNAL:9094`
- Initialized the MinIO Data Lake (`raw` bucket) with:
	- The unedited Porto CSV
	- 3 months of NYC Parquet files for future batch ETL processing

### 2. Geospatial Engineering (PySpark)

- Conducted Exploratory Data Analysis (EDA) on the Porto dataset using Jupyter Notebooks.
- Applied a mathematical linear transformation in PySpark to "teleport" the Porto GPS trajectories into a tightly fitted bounding box covering Casablanca's urban landmass.
- Filtered out "submarine" anomalies and generated a cleaned `casablanca_teleported.csv` mapped to Casablanca's 16 arrondissements.
- Validated the transformation visually using a generated Folium interactive map.

### 3. The Real-Time Simulation Layer

Engineered three distinct Python Kafka Producers to synthesize a live, chaotic city environment, strictly adhering to Capstone requirements:

#### `vehicle_gps_producer.py` (The Fleet)

- Replays the teleported Casablanca trajectories at accelerated speed.
- Engineered realism: Injects Gaussian noise ($\sigma \approx 20m$) to simulate inaccurate mobile GPS sensors.
- Event-time manipulation: Integrates a 5% "blackout" probability by dynamically subtracting 60-180 seconds from the payload timestamp, simulating network drops to test Flink watermarks downstream.

#### `trip_request_producer.py` (The Demand)

- Generates synthetic citizen ride requests between Casablanca's 16 zones.
- Dynamic demand curve: Uses datetime logic to alter event emission rate based on the actual clock.
- Simulates 3x-5x demand spikes during rush hours (7-9 AM, 5-7 PM) and reduced demand during Friday afternoons.

