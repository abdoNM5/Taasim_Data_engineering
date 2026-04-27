# -*- coding: utf-8 -*-
from kafka import KafkaProducer
import json
import csv
import time
import random # Added for noise and blackouts

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9094'], 
    api_version=(2, 5, 0),
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'raw.gps'
csv_file_path = 'casablanca_real_roads_final.csv' 

print(f" Starting ADVANCED live GPS stream to Kafka topic: '{topic_name}'...")
try:
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                polyline = json.loads(row["CASA_POLYLINE"])
                if not polyline:
                    continue
                
                for coord in polyline:
                    longitude, latitude = coord[0], coord[1]
                    
                    
                    noisy_lat = latitude + random.gauss(0, 0.0002)
                    noisy_lon = longitude + random.gauss(0, 0.0002)
                    
                    # 2. The Blackout Anomaly (5% chance to delay the event time by 60-180s) 
                    event_time = int(row["TIMESTAMP"])
                    if random.random() < 0.05:
                        event_time -= random.randint(60, 180) 
                    
                    
                    gps_payload = {
                        "taxi_id": int(row["TAXI_ID"]),    
                        "timestamp": event_time, 
                        "latitude": round(noisy_lat, 6),
                        "longitude": round(noisy_lon, 6),
                        "speed": round(random.uniform(10.0, 60.0), 1), # Mocked speed in km/h
                        "status": "AVAILABLE"
                    }
                    
                     
                    producer.send(topic_name, key=row["TAXI_ID"], value=gps_payload)
                    print(f"Sent: {gps_payload}")
                    
                    time.sleep(0.05) 
                
            except (json.JSONDecodeError, ValueError, IndexError) as e:
                continue
                
except KeyboardInterrupt:
    print("\n Live stream paused manually")
except Exception as e:
    print(f"\n ERROR: {e}")
finally:
    producer.flush()
    producer.close()
    print("KAFKA PRODUCER CLOSED")