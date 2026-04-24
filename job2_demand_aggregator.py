import os, urllib.request, sys
from pathlib import Path
from pyflink.find_flink_home import _find_flink_home
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.connectors.cassandra import CassandraSink
from pyflink.datastream.functions import AggregateFunction, WindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Duration, Time
import json
import time

print("Initializing Flink Environment for Job 2...")
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

flink_lib = Path(_find_flink_home()) / "lib"
jars = {
    "flink-connector-kafka-3.0.2-1.18.jar": "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.0.2-1.18/flink-connector-kafka-3.0.2-1.18.jar",
    "kafka-clients-3.2.3.jar": "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar",
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
env.set_python_executable(sys.executable)
env.set_parallelism(1)

kafka_props = {
    'bootstrap.servers': '127.0.0.1:9094',
    'group.id': 'taasim-job2-demand',
    'auto.offset.reset': 'earliest'
}

gps_stream = env.add_source(FlinkKafkaConsumer('processed.gps', SimpleStringSchema(), properties=kafka_props))
trips_stream = env.add_source(FlinkKafkaConsumer('raw.trips', SimpleStringSchema(), properties=kafka_props))

def parse_gps_json(data_string):
    try:
        data = json.loads(data_string)
        return ("vehicle", int(data['zone_id']), int(data['event_time']), str(data['taxi_id']), str(data.get('status', 'UNKNOWN')))
    except Exception:
        return None

def parse_trips_json(data_string):
    try:
        data = json.loads(data_string)
        return ("trip", int(data['origin_zone']), int(data['requested_at']), str(data['trip_id']), "PENDING")
    except Exception:
        return None

gps_parsed = gps_stream.map(
    parse_gps_json,
    output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG(), Types.STRING(), Types.STRING()])
).filter(lambda x: x is not None)

trips_parsed = trips_stream.map(
    parse_trips_json,
    output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG(), Types.STRING(), Types.STRING()])
).filter(lambda x: x is not None)

class EventTimeExtractor(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value[2] * 1000)

watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(3)).with_timestamp_assigner(EventTimeExtractor())

combined_stream = gps_parsed.union(trips_parsed)
event_time_stream = combined_stream.assign_timestamps_and_watermarks(watermark_strategy)

keyed_stream = event_time_stream.key_by(lambda record: int(record[1]))

# Using TumblingEventTimeWindows for PyFlink 1.18 compatibility
windowed_stream = keyed_stream.window(TumblingEventTimeWindows.of(Time.seconds(30)))

class DemandAccumulator:
    def __init__(self):
        self.vehicle_ids = set()
        self.trip_ids = set()
        self.zone_id = None

class DemandAggregateFunction(AggregateFunction):
    def create_accumulator(self):
        return DemandAccumulator()
    
    def add(self, value, accumulator):
        event_type, zone_id, _, entity_id, _ = value
        accumulator.zone_id = zone_id
        if event_type == "vehicle":
            accumulator.vehicle_ids.add(entity_id)
        elif event_type == "trip":
            accumulator.trip_ids.add(entity_id)
        return accumulator
    
    def get_result(self, accumulator):
        vehicle_count = len(accumulator.vehicle_ids)
        trip_count = len(accumulator.trip_ids)
        ratio = trip_count / max(vehicle_count, 1)
        return (accumulator.zone_id, vehicle_count, trip_count, float(ratio))
    
    def merge(self, acc_a, acc_b):
        acc_a.vehicle_ids.update(acc_b.vehicle_ids)
        acc_a.trip_ids.update(acc_b.trip_ids)
        return acc_a

class DemandWindowFunction(WindowFunction):
    def apply(self, key, window, inputs):
        result = None
        for agg_result in inputs:
            result = agg_result
            break
        if result:
            yield (result[0], result[1], result[2], result[3], int(window.start))

windowed_aggregated = windowed_stream.aggregate(
    DemandAggregateFunction(),
    window_function=DemandWindowFunction(),
    output_type=Types.TUPLE([Types.INT(), Types.INT(), Types.INT(), Types.DOUBLE(), Types.LONG()])
)

def format_for_cassandra(record):
    return ("Casablanca", int(record[0]), int(record[4]), int(record[1]), int(record[2]), float(record[3]))

cassandra_stream = windowed_aggregated.map(
    format_for_cassandra,
    output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG(), Types.INT(), Types.INT(), Types.DOUBLE()])
)

cassandra_query = "INSERT INTO taasim.demand_zones (city, zone_id, window_start, active_vehicles, pending_requests, ratio) VALUES (?, ?, ?, ?, ?, ?);"
CassandraSink.add_sink(cassandra_stream).set_host("127.0.0.1", 9042).set_query(cassandra_query).build()

def tuple_to_demand_json(record):
    return json.dumps({
        "schema_version": "1.0.0", "event_type": "DEMAND_AGGREGATED",
        "zone_id": int(record[0]), "active_vehicles": int(record[1]),
        "pending_requests": int(record[2]), "supply_demand_ratio": float(record[3]),
        "window_start_ms": int(record[4]), "ingested_at": int(time.time() * 1000),
        "city": "Casablanca"
    })

kafka_sink = FlinkKafkaProducer("processed.demand", SimpleStringSchema(), producer_config={"bootstrap.servers": "127.0.0.1:9094", "transaction.timeout.ms": "900000"})
windowed_aggregated.map(tuple_to_demand_json, output_type=Types.STRING()).add_sink(kafka_sink)

print("Submitting Job 2...")
env.execute("Standalone: Job 2 - Demand Aggregation")
