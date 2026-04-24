# -*- coding: utf-8 -*-
from kafka import KafkaProducer
import json
import time
import random
import uuid 
from datetime import datetime

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9094'],
    api_version=(2, 5, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'raw.trips'

print(f"Starting OFFICIAL TaaSim Trip Request Simulator to topic: '{topic_name}'...")


try:
    while True:
        
        trip_request = {
            "trip_id": str(uuid.uuid4()), 
            "rider_id": random.randint(1000, 9999),
            "origin_zone": random.randint(1, 16),      
            "destination_zone": random.randint(1, 16),
            "requested_at": int(time.time()),
            "call_type": random.choice(['A', 'B', 'C']) # Matching Porto dataset
        }

        
        producer.send(topic_name, value=trip_request)
        
        current_time = datetime.now()
        print(f"[{current_time.strftime('%H:%M:%S')}] 📱 Rider {trip_request['rider_id']} hailed a Taxi (Type {trip_request['call_type']}) from Zone {trip_request['origin_zone']} to Zone {trip_request['destination_zone']}!")

        
        base_sleep = 3.0 # A normal request happens every 3 seconds
        current_hour = current_time.hour
        is_friday = current_time.weekday() == 4
        
        # RPeak hours (7-9h, 17-19h) -> 3x to 5x FASTER 
        if (7 <= current_hour <= 9) or (17 <= current_hour <= 19):
            multiplier = random.uniform(3.0, 5.0)
            actual_sleep = base_sleep / multiplier
            
        #Friday 12-14h -> REDUCED rate
        elif is_friday and (12 <= current_hour <= 14):
            multiplier = 0.5 
            actual_sleep = base_sleep / multiplier 
            
        #Normal off-peak
        else:
            multiplier = 1.0
            actual_sleep = base_sleep

        
        time.sleep(actual_sleep)

except KeyboardInterrupt:
    print("\n🛑 Trip simulator paused manually.")
except Exception as e:
    print(f"\n❌ ERROR: {e}")
finally:
    producer.flush()
    producer.close()
    print("KAFKA PRODUCER CLOSED")