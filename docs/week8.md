# Week 8: Demo + Investor Pitch

## Overview
Week 8 is the culmination of the TaaSim Data Engineering Capstone project. The focus is on polishing the presentation, finalizing the technical documentation, and preparing the real-time simulation components for the final demo.

## Deliverables Implemented

### 1. Demand Spike Simulation
- **File:** `src/producers/event_injector.py`
- **Functionality:** Injects a burst of trip requests to the `raw.trips` Kafka topic to simulate a morning rush hour. This allows us to visually demonstrate the platform's real-time matching and heatmap responsiveness on Grafana during the live demo.

### 2. Pitch Deck Outline
- **File:** `docs/Pitch_Deck_Outline.md`
- **Details:** A comprehensive 10-slide outline structured to pitch the platform to investors. It covers the problem statement, the Kappa architecture solution, the ML demand forecasting "brain", live demo flow, and the business model.

### 3. Technical Report
- **File:** `docs/Technical_Report.md`
- **Details:** The foundational outline for the 12-15 page final technical document. It details the architecture decisions, Flink streaming pipeline details, Cassandra database modeling, Spark MLlib forecasting, and non-functional requirement (SLA) measurements.

## Demo Preparation Steps
1. **Start the Infrastructure:** Bring up all components via `docker-compose up -d`.
2. **Access Grafana Dashboard:** Navigate to `localhost:3000` to load the demand heatmap and live vehicle positions.
3. **Run Real-Time Feeds:** Execute standard producers to start regular system load.
4. **Trigger the Anomaly:** Run `python src/producers/event_injector.py` to flood the system with concurrent trip requests and monitor the latency and heatmap updates dynamically.
