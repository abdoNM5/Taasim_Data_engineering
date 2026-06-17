# Week 5: Batch ETL + Spark Analytics

This document details the tasks for Week 5, focusing on bringing the core end-to-end batch pipeline into operation using Apache Spark.

---

## 🎯 Engineering Tasks

### 1. Spark ETL Job (Porto/Casablanca Data)
*   **Source**: Read Porto CSV from MinIO `raw/` OR the existing `data/raw/casablanca_real_roads_final.csv`.
*   **Processing**:
    *   Apply zone remapping (if Porto).
    *   Parse polyline data (if Casablanca).
    *   Deduplicate records.
    *   Compute **H3 zone IDs** for geospatial indexing.
*   **Sink**: Write clean Parquet files to MinIO `curated/` or `data/processed/`.

### 2. Spark ETL on NYC TLC
*   **Source**: Read the 3-month NYC TLC Parquet dataset.
*   **Processing**: Compute per-zone demand aggregates to simulate large-scale data handling.
*   **Sink**: Write to `curated/`.

### 3. Spark SQL Analytics (Weekly KPIs)
Develop Spark SQL transformations to compute the following metrics:
*   **Trips per Zone**: Total volume of activity.
*   **Avg Trip Duration**: Efficiency metric.
*   **Peak Demand Hours**: Temporal analysis for scheduling.
*   **Coverage Gap**: Zones with demand but < 2 available vehicles.

### 4. Load KPI Aggregates
*   **Persistence**: Load the computed KPI results into the Cassandra `demand_zones` table.
*   **Visualization**: Configure Grafana KPI panels to display corridor demand and peak hours.

---

## ✅ Deliverables
1.  **✓ Highly Optimized ETL**: Process 1.7M+ rows in under 5 minutes.
2.  **✓ Scalable Pipeline**: Successfully process NYC TLC data (10M rows/month).
3.  **✓ Operational Insight**: Grafana KPI panel showing live/historical demand patterns.

---

## 💡 Notes for Existing Data
Since you already have `casablanca_real_roads_final.csv` in `data/raw/`, you can prioritize mapping your Spark ETL job to this specific dataset. The `CASA_POLYLINE` contains the map-matched coordinates which can be used to derive trip distances and zone IDs.
