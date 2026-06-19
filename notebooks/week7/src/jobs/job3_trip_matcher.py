import os, sys, json, time
import urllib.request

# Force Java and PyFlink to use IPv4 to prevent gRPC Multiplexer hanging up on Windows
os.environ["_JAVA_OPTIONS"] = "-Djava.net.preferIPv4Stack=true"
from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment

from pyflink.datastream.connectors.cassandra import CassandraSink
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream import EmbeddedRocksDBStateBackend, CheckpointingMode
from pyflink.find_flink_home import _find_flink_home
import math

print("Initializing Flink Environment for Job 3 (Trip Matcher)...")
if os.name == "nt":
    for home in [
        r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot",
        r"C:\Program Files\Adoptium\jdk-17.0.7",
        r"C:\Program Files\Java\jdk-17",
    ]:
        java_bin = Path(home) / "bin" / "java.exe"
        if java_bin.exists():
            os.environ["JAVA_HOME"] = str(Path(home))
            os.environ["PATH"] = f"{java_bin.parent}{os.pathsep}" + os.environ.get("PATH", "")
            break
elif os.name == "posix":
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"

flink_lib = Path(_find_flink_home()) / "lib"
jars = {
    "flink-sql-connector-kafka-3.0.2-1.18.jar": "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar",
    "flink-connector-cassandra_2.12-3.2.0-1.18.jar": "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/3.2.0-1.18/flink-connector-cassandra_2.12-3.2.0-1.18.jar",
}
for jar_name, url in jars.items():
    dest = flink_lib / jar_name
    if not dest.exists():
        print(f"Downloading {jar_name} ...")
        urllib.request.urlretrieve(url, dest)

os.environ["PYFLINK_CLIENT_EXECUTABLE"] = sys.executable
os.environ["PYFLINK_PYTHON_EXECUTABLE"] = sys.executable

env = StreamExecutionEnvironment.get_execution_environment()
for jar_name in jars.keys():
    env.add_jars(f"file://{flink_lib / jar_name}")
env.set_python_executable(sys.executable)
env.set_parallelism(1)

# --- CDC Requirement: RocksDB & Checkpointing ---
print("Configuring Checkpointing (RocksDB disabled for Windows stability)...")
# Note: EmbeddedRocksDBStateBackend has native memory access violations on Windows PyFlink 1.18.
# We comment it out for local testing so the job actually runs, but the code is here for production/Linux.
# env.set_state_backend(EmbeddedRocksDBStateBackend())
# Checkpointing disabled for local Docker because it requires a shared file system between JM and TM
# env.enable_checkpointing(60000, CheckpointingMode.EXACTLY_ONCE)
# checkpoint_config = env.get_checkpoint_config()
# ckpt_path = os.path.abspath("cache/checkpoints/job3")
# if not os.path.exists(ckpt_path):
#     os.makedirs(ckpt_path)
# checkpoint_config.set_checkpoint_storage_dir(f"file:///{ckpt_path.replace(os.sep, '/')}")
# -----------------------------------------------

from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

gps_kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("processed.gps") \
    .set_group_id("taasim-job3-matcher-gps") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

trips_kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("raw.trips") \
    .set_group_id("taasim-job3-matcher-trips") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

gps_stream = env.from_source(gps_kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source GPS")
trips_stream = env.from_source(trips_kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source Trips")

def parse_combined(data_string, src):
    try:
        data = json.loads(data_string)
        # Type, City(Key), payload
        return (src, "Casablanca", data_string)
    except Exception:
        return None

gps_parsed = gps_stream.map(
    lambda x: parse_combined(x, "gps"),
    output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING()])
).filter(lambda x: x is not None)

trips_parsed = trips_stream.map(
    lambda x: parse_combined(x, "trip"),
    output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING()])
).filter(lambda x: x is not None)

combined_stream = gps_parsed.union(trips_parsed)
keyed_stream = combined_stream.key_by(lambda record: record[1], key_type=Types.STRING()) # Key by City ("Casablanca")

class TripMatcherFunction(KeyedProcessFunction):
    def open(self, runtime_context):
        # Map of vehicle_id -> {"zone_id": int, "lat": float, "lon": float, "last_updated": long, "status": str}
        self.vehicles_state = runtime_context.get_map_state(
            MapStateDescriptor("vehicles", Types.STRING(), Types.STRING())
        )
        # Map of trip_id -> {"origin_zone": int, "dest_zone": int, "rider_id": int, "requested_at": long, "fallback_attempted": bool}
        self.pending_trips = runtime_context.get_map_state(
            MapStateDescriptor("pending_trips", Types.STRING(), Types.STRING())
        )

    def process_element(self, value, ctx):
        src_type, city, payload_str = value
        payload = json.loads(payload_str)
        current_time = int(time.time() * 1000)

        if src_type == "gps":
            v_id = str(payload.get('taxi_id'))
            v_data = {
                "zone_id": payload.get('zone_id', 0),
                "lat": payload.get('centroid_lat', payload.get('latitude', 0.0)),
                "lon": payload.get('centroid_lon', payload.get('longitude', 0.0)),
                "speed": payload.get('speed_kmh', payload.get('speed', 30.0)),
                "last_updated": current_time, # Use arrival time to avoid 2013 dataset timestamps being marked as 13-years stale
                "status": payload.get('status', 'AVAILABLE')
            }
            self.vehicles_state.put(v_id, json.dumps(v_data))
        
        elif src_type == "trip":
            trip_id = str(payload.get('trip_id'))
            origin_zone = int(payload.get('origin_zone', 0))
            
            # Try to find a vehicle in the EXACT zone immediately
            matched_vehicle, eta = self.find_vehicle_in_zone(origin_zone, payload)
            
            if matched_vehicle:
                # Match successful
                yield self.emit_match(trip_id, payload, matched_vehicle, eta, "MATCHED_EXACT")
                # Remove vehicle so it's not double-booked
                self.vehicles_state.remove(matched_vehicle)
            else:
                # Need to wait 5 seconds (5000 ms) for fallback
                payload['fallback_attempted'] = False
                self.pending_trips.put(trip_id, json.dumps(payload))
                
                # Register timer for 5 seconds from now
                ctx.timer_service().register_processing_time_timer(ctx.timer_service().current_processing_time() + 5000)

    def on_timer(self, timestamp, ctx):
        keys_to_remove = []
        
        # Realize the iterator into a list to avoid concurrent modification issues
        pending_items = []
        for k, v in self.pending_trips.items():
            pending_items.append((k, v))
            
        for trip_id, trip_str in pending_items:
            trip_payload = json.loads(trip_str)
            origin_zone = int(trip_payload.get('origin_zone', 0))
            
            # Try exact zone again just in case a vehicle became available
            matched_vehicle, eta = self.find_vehicle_in_zone(origin_zone, trip_payload)
            
            if matched_vehicle:
                yield self.emit_match(trip_id, trip_payload, matched_vehicle, eta, "MATCHED_DELAYED_EXACT")
                self.vehicles_state.remove(matched_vehicle)
                keys_to_remove.append(trip_id)
            else:
                # Fallback: Expand to adjacent zones (e.g., origin_zone - 1, origin_zone + 1)
                adjacent_zones = [origin_zone - 1, origin_zone + 1]
                fallback_matched = None
                fallback_eta = 0
                
                for adj_zone in adjacent_zones:
                    v, e = self.find_vehicle_in_zone(adj_zone, trip_payload)
                    if v:
                        fallback_matched = v
                        # Penalize ETA for adjacent zone match (add 5 mins / 300 seconds)
                        fallback_eta = e + 300 
                        break
                
                if fallback_matched:
                    yield self.emit_match(trip_id, trip_payload, fallback_matched, fallback_eta, "MATCHED_FALLBACK")
                    self.vehicles_state.remove(fallback_matched)
                    keys_to_remove.append(trip_id)
                else:
                    # If still no match, mark as unfulfilled or keep waiting. We will just mark unfulfilled.
                    yield self.emit_match(trip_id, trip_payload, "NONE", 0, "UNFULFILLED")
                    keys_to_remove.append(trip_id)
        
        for k in keys_to_remove:
            self.pending_trips.remove(k)

    def find_vehicle_in_zone(self, target_zone, trip_payload=None):
        """Finds a vehicle in the target zone. Returns (vehicle_id, base_eta)."""
        current_time = int(time.time() * 1000)
        best_v = None
        v_speed = 30.0 # Default fallback speed
        v_lat, v_lon = 0.0, 0.0
        
        for v_id, v_str in self.vehicles_state.items():
            try:
                v_data = json.loads(v_str)
                stale_diff = current_time - v_data.get('last_updated', 0)
                
                # Only match available vehicles, and filter out stale ones (e.g., older than 2 minutes)
                if v_data.get('status') in ['AVAILABLE', 'EN_ROUTE'] and stale_diff < 120000:
                    z_id = v_data.get('zone_id')
                    if z_id is not None and int(z_id) == int(target_zone):
                        best_v = v_id
                        v_speed = float(v_data.get('speed', 30.0))
                        v_lat, v_lon = float(v_data.get('lat', 0.0)), float(v_data.get('lon', 0.0))
                        break
            except Exception:
                pass
        
        if best_v:
            # Simple heuristic ETA: (Distance / Speed)
            # If we have trip origin coordinates, we can be more precise.
            # For now, use a zone-based heuristic as per CDC
            eta = 180 # Default 3 mins
            if trip_payload and 'origin_lat' in trip_payload:
                # Calculate Haversine distance
                o_lat, o_lon = trip_payload['origin_lat'], trip_payload['origin_lon']
                dist_km = self.haversine(o_lat, o_lon, v_lat, v_lon)
                # ETA in seconds = (km / (km/h)) * 3600
                eta = int((dist_km / max(v_speed, 10.0)) * 3600)
            
            return best_v, max(eta, 60) # Minimum 1 minute ETA
        return None, 0

    def haversine(self, lat1, lon1, lat2, lon2):
        R = 6371 # Earth radius in km
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def emit_match(self, trip_id, trip_payload, vehicle_id, eta, status):
        output_record = {
            "city": "Casablanca",
            "date_bucket": time.strftime("%Y-%m-%d"),
            "created_at": trip_payload.get('requested_at', int(time.time() * 1000)),
            "trip_id": trip_id,
            "rider_id": trip_payload.get('rider_id', 0),
            "origin_zone": trip_payload.get('origin_zone', 0),
            "destination_zone": trip_payload.get('destination_zone', 0),
            "call_type": trip_payload.get('call_type', 'A'),
            "matched_taxi": vehicle_id,
            "eta_seconds": eta,
            "status": status
        }
        return json.dumps(output_record)

matched_trips_json = keyed_stream.process(TripMatcherFunction(), output_type=Types.STRING())

def format_for_cassandra(record_str):
    try:
        record = json.loads(record_str)
        rider_id = record.get("rider_id", 0)
        if isinstance(rider_id, str) and "RIDER-SPIKE-" in rider_id:
            rider_id = int(rider_id.replace("RIDER-SPIKE-", ""))
        return [(
            record["city"], 
            record["date_bucket"], 
            int(record["created_at"]), 
            str(record["trip_id"]), 
            int(rider_id), 
            int(record["origin_zone"]), 
            int(record["destination_zone"]), 
            str(record["call_type"]),
            str(record["matched_taxi"]),
            int(record["eta_seconds"]),
            str(record["status"])
        )]
    except Exception:
        return []

cassandra_stream = matched_trips_json.flat_map(
    format_for_cassandra,
    output_type=Types.TUPLE([
        Types.STRING(), Types.STRING(), Types.LONG(), Types.STRING(), Types.INT(), 
        Types.INT(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING()
    ])
)

cassandra_query = """
    INSERT INTO taasim.trips 
    (city, date_bucket, created_at, trip_id, rider_id, origin_zone, destination_zone, call_type, matched_taxi, eta_seconds, status) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
CassandraSink.add_sink(cassandra_stream).set_host("cassandra", 9042).set_query(cassandra_query).build()

from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
kafka_sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_record_serializer(KafkaRecordSerializationSchema.builder() \
        .set_topic("processed.trips") \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build() \
    ) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()
matched_trips_json.sink_to(kafka_sink).name("Kafka Sink")
matched_trips_json.print()

print("Submitting Job 3 (Trip Matcher)...")
env.execute("Standalone: Job 3 - Trip Matching")
