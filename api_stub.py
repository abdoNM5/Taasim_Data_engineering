import json
import time
import uuid
import threading
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer, KafkaConsumer
import uvicorn

app = FastAPI(title="TaaSim API Stub")

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

if __name__ == "__main__":
    print("Starting API Stub on port 8001...")
    uvicorn.run(app, host="0.0.0.0", port=8001)
