# TaaSim Pitch Deck Outline
**Theme:** "Build the data platform that moves Casablanca."

## Slide 1: Title Slide
- **Project Name:** TaaSim
- **Tagline:** Transport as a Service — Urban Mobility Platform for Casablanca
- **Team Names & Roles**

## Slide 2: The Problem
- **Fragmented Mobility:** No shared data layer between grand taxis, petits taxis, and transit options.
- **Demand Blindness:** Inefficient cruising for drivers, long waits without visibility for passengers.
- **Unmapped Suburbs:** Rapidly growing periphery lacking formal transit routes.

## Slide 3: The Solution
- **TaaSim Platform:** A data-driven layer connecting supply and demand in real-time.
- **Dynamic Matching:** Matching riders to nearest vehicles instantly.
- **Proactive Management:** Using ML to forecast demand surges before they happen.

## Slide 4: Architecture Overview
- **Kappa Architecture:** Unified streaming pipeline handling both live events and historical replays.
- **Key Technologies:** Kafka (Event Bus), Flink (Stream Processing), Cassandra (Low Latency Storage), Spark (Batch/ML), FastAPI & Grafana (Serving & Dashboard).

## Slide 5: The Data Pipeline in Action
- **Ingestion:** GPS pings and trip reservations stream into Kafka.
- **Real-Time Layer:** Flink handles GPS anonymization, map-matching, demand aggregation, and ETA calculation.
- **Serving Layer:** Cassandra provides millisecond reads for the API and Live Map.

## Slide 6: Demand Forecasting (The Brain)
- **Spark MLlib:** Gradient Boosted Trees model trained on historical patterns.
- **Features Used:** Weather (Open-Meteo), Spatial (Casablanca Zones), and Temporal data.
- **Impact:** Allows repositioning of idle vehicles 30 minutes ahead of demand.

## Slide 7: Live Demo 
- *Switch to screen share*
- **Step 1:** Show live vehicle movement on Grafana map.
- **Step 2:** Reserve a trip via the API, show immediate Flink matching.
- **Step 3:** Inject a demand spike (morning rush in Bouskoura) using `event_injector.py` and watch the heatmap react.

## Slide 8: Key Metrics & SLAs
- **Performance:** Sub-5 second trip match latency; 15-second GPS freshness.
- **Scalability:** Cassandra handles thousands of concurrent reads.
- **Reliability:** Flink Checkpointing to MinIO ensures zero data loss during node failures.

## Slide 9: Business Model & Future Scope
- **B2B / B2G Subscriptions:** Selling mobility analytics to the Casablanca City Council and urban planners.
- **Driver Commission:** Small transaction fee on matched trips.
- **Future Integration:** Adding tramway delays and smart ticketing.

## Slide 10: Conclusion & Q&A
- **Vision:** Transforming Casablanca into a proactive, data-driven smart city.
- **Thank You!**
