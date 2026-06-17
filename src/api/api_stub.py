import json
import time
import uuid
import threading
import os
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer, KafkaConsumer
import uvicorn

# MUST be set before importing pyspark
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell"
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml import PipelineModel
import pyspark.sql.functions as F

app = FastAPI(title="TaaSim API Stub")

print("Initializing SparkSession... (This may take ~10 seconds)")

spark = SparkSession.builder \
    .appName("TaaSim-API") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("Loading ML model from MinIO...")
ml_model = PipelineModel.load("s3a://machinel/models/demand_v1/")
print("ML model loaded successfully!")

# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# In-memory storage for trip status (populated by a background consumer thread)
trip_statuses = {}

def consume_processed_trips():
    while True:
        try:
            consumer = KafkaConsumer(
                'processed.trips',
                bootstrap_servers=['127.0.0.1:9094'],
                auto_offset_reset='latest',
                group_id='api-stub-consumer',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            for message in consumer:
                try:
                    trip_data = message.value
                    trip_id = trip_data.get('trip_id')
                    if trip_id:
                        trip_statuses[trip_id] = trip_data
                except Exception as msg_error:
                    print(f"Error parsing message: {msg_error}")
        except Exception as e:
            print(f"Error consuming processed.trips: {e}. Retrying in 3 seconds...")
            time.sleep(3)

# Start background consumer
thread = threading.Thread(target=consume_processed_trips, daemon=True)
thread.start()

class TripRequest(BaseModel):
    rider_id: int
    origin_zone: int
    destination_zone: int
    call_type: str = 'A'

class ForecastRequest(BaseModel):
    zone_id: int
    datetime: str  # e.g., "2026-06-16T18:00:00"

@app.get("/")
def read_root():
    return {"message": "Welcome to TaaSim API Stub! Use /reserve_trip to book a ride."}

@app.post("/reserve_trip")
def reserve_trip(request: TripRequest):
    trip_id = str(uuid.uuid4())
    payload = {
        "trip_id": trip_id,
        "rider_id": request.rider_id,
        "origin_zone": request.origin_zone,
        "destination_zone": request.destination_zone,
        "call_type": request.call_type,
        "requested_at": int(time.time() * 1000)
    }
    
    # Send to Kafka
    producer.send('raw.trips', payload)
    producer.flush()
    
    # Pre-populate status as pending
    trip_statuses[trip_id] = {"status": "PENDING", "trip_id": trip_id}
    
    return {"trip_id": trip_id, "status": "PENDING", "message": "Trip reserved, finding a driver..."}

@app.get("/trip_status/{trip_id}")
def get_trip_status(trip_id: str):
    if trip_id not in trip_statuses:
        raise HTTPException(status_code=404, detail="Trip not found")
    
    return trip_statuses[trip_id]

@app.post("/api/demand/forecast")
def get_demand_forecast(request: ForecastRequest):
    # Parse datetime
    try:
        dt = datetime.fromisoformat(request.datetime)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid datetime format. Use ISO format, e.g., 2026-06-16T18:00:00")
        
    hour = dt.hour
    day_of_week = dt.weekday() # 0 = Monday, 6 = Sunday
    is_weekend = 1.0 if day_of_week >= 5 else 0.0
    is_friday = 1.0 if day_of_week == 4 else 0.0
    
    # We set lag features to 0.0 as defaults, as explained in the plan
    row = Row(
        zone_id=float(request.zone_id),
        hour_of_day=float(hour),
        day_of_week=float(day_of_week),
        is_weekend=is_weekend,
        is_friday=is_friday,
        is_raining=0.0,
        demand_lag_1d=0.0,
        demand_lag_7d=0.0,
        rolling_7d_mean=0.0
    )
    
    df = spark.createDataFrame([row])
    
    # Transform
    predictions = ml_model.transform(df)
    pred_val = predictions.select("prediction").collect()[0][0]
    
    return {
        "zone_id": request.zone_id,
        "datetime": request.datetime,
        "predicted_demand": max(0.0, float(pred_val)) # Demand cannot be negative
    }

if __name__ == "__main__":
    print("Starting API Stub on port 8001...")
    uvicorn.run(app, host="0.0.0.0", port=8001)
