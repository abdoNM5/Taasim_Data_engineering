import os, urllib.request, sys
from pathlib import Path

# Force Java and PyFlink to use IPv4 to prevent gRPC Multiplexer hanging up on Windows
os.environ["_JAVA_OPTIONS"] = "-Djava.net.preferIPv4Stack=true"
from pyflink.find_flink_home import _find_flink_home
from pyflink.datastream import StreamExecutionEnvironment

from pyflink.datastream.connectors.cassandra import CassandraSink
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Duration
import json
import time

print("Initializing Flink Environment...")
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
    linux_java_path = "/usr/lib/jvm/java-21-openjdk-amd64"
    if Path(linux_java_path).exists():
        os.environ["JAVA_HOME"] = linux_java_path

flink_lib = Path(_find_flink_home()) / "lib"
jars = {
    "flink-connector-kafka-3.0.2-1.18.jar": "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.0.2-1.18/flink-connector-kafka-3.0.2-1.18.jar",
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

from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("raw.gps") \
    .set_group_id("taasim-job1-gps") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

raw_stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

def parse_gps_json(data_string): 
    try:
        data = json.loads(data_string)
        return (int(data['taxi_id']), int(data['timestamp']), float(data['latitude']), float(data['longitude']), float(data['speed']), str(data['status']))
    except Exception:
        return None

structured_stream = raw_stream.map(
    parse_gps_json,
    output_type=Types.TUPLE([Types.INT(), Types.LONG(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.STRING()])
).filter(lambda x: x is not None)

class GPSTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value[1] * 1000)

watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(3)).with_timestamp_assigner(GPSTimestampAssigner())
event_time_stream = structured_stream.assign_timestamps_and_watermarks(watermark_strategy)

def is_valid_gps(record):
    return record[4] <= 150.0 and 33.4 <= record[2] <= 33.7 and -7.8 <= record[3] <= -7.4

valid_stream = event_time_stream.filter(is_valid_gps)

CASABLANCA_ZONES = {
    1: {"name": "Anfa", "min_lat": 33.58, "max_lat": 33.62, "min_lon": -7.66, "max_lon": -7.62, "centroid_lat": 33.60, "centroid_lon": -7.64},
    2: {"name": "Sidi Belyout", "min_lat": 33.58, "max_lat": 33.61, "min_lon": -7.62, "max_lon": -7.58, "centroid_lat": 33.595, "centroid_lon": -7.60},
    3: {"name": "Roches Noires", "min_lat": 33.60, "max_lat": 33.64, "min_lon": -7.58, "max_lon": -7.54, "centroid_lat": 33.62, "centroid_lon": -7.56},
    4: {"name": "Ain Sebaa", "min_lat": 33.64, "max_lat": 33.70, "min_lon": -7.54, "max_lon": -7.48, "centroid_lat": 33.67, "centroid_lon": -7.51},
    5: {"name": "Hay Mohammadi", "min_lat": 33.60, "max_lat": 33.65, "min_lon": -7.56, "max_lon": -7.50, "centroid_lat": 33.625, "centroid_lon": -7.53},
    6: {"name": "Mers Sultan", "min_lat": 33.57, "max_lat": 33.60, "min_lon": -7.62, "max_lon": -7.58, "centroid_lat": 33.585, "centroid_lon": -7.60},
    7: {"name": "Maarif", "min_lat": 33.57, "max_lat": 33.60, "min_lon": -7.64, "max_lon": -7.60, "centroid_lat": 33.585, "centroid_lon": -7.62},
    8: {"name": "Sidi Othmane", "min_lat": 33.55, "max_lat": 33.59, "min_lon": -7.58, "max_lon": -7.52, "centroid_lat": 33.57, "centroid_lon": -7.55},
    9: {"name": "Sbata", "min_lat": 33.56, "max_lat": 33.59, "min_lon": -7.56, "max_lon": -7.50, "centroid_lat": 33.575, "centroid_lon": -7.53},
    10: {"name": "Ben M'Sick", "min_lat": 33.55, "max_lat": 33.58, "min_lon": -7.52, "max_lon": -7.46, "centroid_lat": 33.565, "centroid_lon": -7.49},
    11: {"name": "Ain Chock", "min_lat": 33.54, "max_lat": 33.58, "min_lon": -7.60, "max_lon": -7.54, "centroid_lat": 33.56, "centroid_lon": -7.57},
    12: {"name": "Hay Hassani", "min_lat": 33.54, "max_lat": 33.58, "min_lon": -7.66, "max_lon": -7.60, "centroid_lat": 33.56, "centroid_lon": -7.63},
    13: {"name": "Sidi Abderrahmane", "min_lat": 33.56, "max_lat": 33.60, "min_lon": -7.70, "max_lon": -7.66, "centroid_lat": 33.58, "centroid_lon": -7.68},
    14: {"name": "Ain Diab", "min_lat": 33.58, "max_lat": 33.62, "min_lon": -7.70, "max_lon": -7.66, "centroid_lat": 33.60, "centroid_lon": -7.68},
    15: {"name": "Dar Bouazza", "min_lat": 33.52, "max_lat": 33.58, "min_lon": -7.74, "max_lon": -7.66, "centroid_lat": 33.55, "centroid_lon": -7.70},
    16: {"name": "Sidi Moumen", "min_lat": 33.62, "max_lat": 33.68, "min_lon": -7.52, "max_lon": -7.46, "centroid_lat": 33.65, "centroid_lon": -7.49},
}

def map_and_anonymize(record):
    city = "Casablanca"
    matched_zone_id = 99
    final_lat, final_lon = record[2], record[3]
    for z_id, z_data in CASABLANCA_ZONES.items():
        if z_data["min_lat"] <= record[2] <= z_data["max_lat"] and z_data["min_lon"] <= record[3] <= z_data["max_lon"]:
            matched_zone_id, final_lat, final_lon = z_id, z_data["centroid_lat"], z_data["centroid_lon"]
            break
    return (city, int(matched_zone_id), int(record[1]), int(record[0]), float(final_lat), float(final_lon), float(record[4]), str(record[5]))

anonymized_stream = valid_stream.map(
    map_and_anonymize,
    output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG(), Types.INT(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING()])
).filter(lambda r: r[1] != 99)

def tuple_to_json(record):
    return json.dumps({
        "schema_version": "1.0.0", "event_type": "GPS_NORMALIZED",
        "event_time": int(record[2]), "ingested_at": int(time.time()),
        "taxi_id": int(record[3]), "zone_id": int(record[1]),
        "centroid_lat": float(record[4]), "centroid_lon": float(record[5]),
        "speed_kmh": float(record[6]), "status": str(record[7])
    })

from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
kafka_sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_record_serializer(KafkaRecordSerializationSchema.builder() \
        .set_topic("processed.gps") \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build() \
    ) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()
anonymized_stream.map(tuple_to_json, output_type=Types.STRING()).sink_to(kafka_sink).name("Kafka Sink")

cassandra_query = "INSERT INTO taasim.vehicle_positions (city, zone_id, event_time, taxi_id, latitude, longitude, speed, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
CassandraSink.add_sink(anonymized_stream).set_host("cassandra", 9042).set_query(cassandra_query).build()

print("Submitting Job...")
env.execute("Standalone: Job 1 - GPS Normalizer")
