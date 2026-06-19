import json
import time
import random
import uuid
import os
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = "raw.trips"
SPIKE_ZONES = [5, 6, 7]  # Simulating a morning rush in specific zones (e.g., residential areas)
NUM_REQUESTS = 150  # Number of trips to inject

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=3,
    )

def inject_demand_spike():
    print(f"Connecting to Kafka at {KAFKA_BROKER}...")
    try:
        producer = create_producer()
        print("Connected successfully. Injecting demand spike...")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return

    for i in range(NUM_REQUESTS):
        origin = random.choice(SPIKE_ZONES)
        destination = random.randint(1, 16)
        while destination == origin:
            destination = random.randint(1, 16)
        
        trip_id = f"SPIKE-{uuid.uuid4().hex[:8]}"
        payload = {
            "trip_id": trip_id,
            "rider_id": f"RIDER-SPIKE-{random.randint(1000, 9999)}",
            "origin_zone": origin,
            "destination_zone": destination,
            "call_type": "A",
            "requested_at": int(time.time() * 1000)
        }
        
        producer.send(TOPIC, key=trip_id, value=payload)
        
        if (i + 1) % 10 == 0:
            print(f"Injected {i + 1} trip requests...")
            time.sleep(0.5)  # Slight delay to emulate a realistic burst
            
    producer.flush()
    print(f"Demand spike completed. {NUM_REQUESTS} requests injected.")

if __name__ == "__main__":
    inject_demand_spike()
