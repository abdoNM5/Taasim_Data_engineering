# Week 7: Security + Integration

## Overview
During Week 7, we focused on securing the TaaSim platform and improving the API infrastructure according to the "Security + Integration" milestone. The API layer has been thoroughly structured, and a security layer using JWT tokens has been implemented.

## What Has Been Implemented

### 1. API Security and Authentication (JWT)
We successfully integrated JWT-based authentication into our FastAPI backend to distinguish between roles and secure the endpoints.
- **Endpoint:** Added a `POST /auth/token` route (in `src/api/routes/auth.py`).
- **Functionality:** Users can request an access token using OAuth2 password flow. The system supports `rider` and `admin` roles, generating a JWT token that encodes the user's role and username.
- **Library:** Uses `python-jose` for JWT encoding and decoding.

### 2. API Restructuring and Modularization
The monolithic API script was refactored into a scalable and modular architecture:
- **Routes (`src/api/routes/`)**: Splitted functionalities into distinct modules:
  - `auth.py`: Handles token generation and authentication.
  - `forecast.py`: Handles demand forecasting using the loaded Spark ML models.
  - `trips.py`: Handles trip requests and interacting with Kafka.
- **Services (`src/api/services/`)**: Separated external service connections:
  - `kafka.py`: Producers and consumers for the event bus.
  - `spark.py`: Spark session management and ML model serving.
- **Core (`src/api/core/`)**: Configurations, logging, and dependencies.

### 3. Docker Integration
- Added a specific `Dockerfile.api` to neatly containerize the FastAPI application.
- Added a `.dockerignore` file to ensure the build context remains light.

## Next Steps / Pending
To fully complete the Week 7 requirements, the following integration items will be covered during the full system review:
- **GPS Anonymization:** Snapping coordinates to zone centroids in Flink Job 1.
- **SLA Measurements:** Benchmarking trip match latencies, ML API response times, and vehicle position freshness.
- **Checkpoint Recovery Validation:** Manually killing a Flink task manager and verifying successful job recovery from MinIO checkpoints.
