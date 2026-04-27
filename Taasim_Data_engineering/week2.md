# Week 2 - Streaming Flow to MinIO (Producer -> Kafka -> Kafka Connect -> MinIO)

## Goal
Explain exactly how events move from Python producers to object files in MinIO, and how each key line in configuration controls storage behavior.

## End-to-End Flow
1. ProducerTrips.py and ProducerGps.py publish JSON events to Kafka topics.
2. Kafka stores records in topic partitions.
3. Kafka Connect S3 Sink reads those topics.
4. Kafka Connect writes batched JSON files to MinIO bucket raw under kafka-archive.

Producers do not write to MinIO directly.

## Producer Layer (what sends records)

### ProducerTrips.py
- Line 10: bootstrap_servers=['localhost:9094']
  - The trips producer connects to Kafka through the external listener mapped to localhost:9094.
- Line 14: topic_name = 'raw.trips'
  - Trips events are published to topic raw.trips.
- Line 32: producer.send(topic_name, value=trip_request)
  - Each generated trip request is sent to Kafka.

### ProducerGps.py
- Line 8: bootstrap_servers=['localhost:9094']
  - GPS producer connects to the same external Kafka listener.
- Line 13: topic_name = 'raw.gps'
  - GPS events are published to topic raw.gps.
- Line 49: producer.send(topic_name, key=row["TAXI_ID"], value=gps_payload)
  - Each coordinate payload is sent to Kafka.

## Kafka Network Layer (why localhost producers can connect)

### docker-compose.yaml
- Line 9: "9094:9094"
  - Exposes Kafka external listener port to host.
- Line 13: KAFKA_LISTENERS includes EXTERNAL://:9094
  - Kafka listens for external producer traffic on 9094.
- Line 14: KAFKA_ADVERTISED_LISTENERS includes EXTERNAL://localhost:9094
  - Kafka tells host-side clients to use localhost:9094.

This is why local Python scripts can publish successfully.

## Kafka Connect Layer (how Kafka records are archived)

### docker-compose.yaml
- Line 132: kafka-connect service definition.
- Line 139: "8083:8083"
  - Exposes Connect REST API on localhost:8083.
- Line 141: CONNECT_BOOTSTRAP_SERVERS: 'kafka:9092'
  - Connect consumes internally from Kafka container network listener.
- Line 161: confluent-hub install ... kafka-connect-s3
  - Installs S3 sink plugin used to write to MinIO.

### connect-s3-raw-archive.json
- Line 4: connector.class = io.confluent.connect.s3.S3SinkConnector
  - Activates S3 sink behavior.
- Line 5: tasks.max = 1
  - One sink task processes assigned topic partitions.
- Line 6: topics = raw.gps,raw.trips
  - Connect subscribes to these two topics.
- Line 7: s3.bucket.name = raw
  - Destination bucket is raw.
- Line 8: topics.dir = kafka-archive
  - All topic folders are created under raw/kafka-archive.
- Line 9: store.url = http://minio:9000
  - Connect writes to MinIO endpoint.
- Line 16: flush.size = 10
  - One object is committed every 10 records per topic-partition.
- Line 17: rotate.interval.ms = 60000
  - Time rotation setting is present, but with DefaultPartitioner this is typically not the active trigger.
- Line 18: key.converter = StringConverter
  - Message keys are interpreted as strings.
- Line 19: value.converter = JsonConverter
  - Values are read as JSON.
- Line 20: value.converter.schemas.enable = false
  - Records are treated as schema-less JSON payloads.

## How MinIO path is built
Given:
- Bucket: raw
- topics.dir: kafka-archive
- Topic: raw.trips
- Partition: 0
- File starting offset: 10

Resulting object path:
raw/kafka-archive/raw.trips/partition=0/raw.trips+0+0000000010.json

Path parts:
- raw -> bucket name
- kafka-archive -> root topic directory
- raw.trips -> topic folder
- partition=0 -> Kafka partition id
- raw.trips+0+0000000010.json -> topic + partition + first offset in that file

## Flush, Partition, and Offsets Explained
- flush.size=10 means each file contains about 10 records per topic-partition.
- That is why file offsets progress as:
  - 0000000000
  - 0000000010
  - 0000000020
- partition=0 means records are currently being written from partition 0.
- If topics had more partitions and records were assigned there, you would also see partition=1, partition=2, etc.

## Why producer logs still appear in terminal
Producer print statements confirm event generation and publish attempts.
They are expected and independent from MinIO writes.
Actual object storage is performed asynchronously by Kafka Connect.

## Practical Plan (Week 2 operations)
1. Start platform with docker compose up -d.
2. Ensure connector is created and RUNNING on localhost:8083.
3. Run ProducerTrips.py and ProducerGps.py.
4. Verify topic ingestion in Kafka.
5. Verify object creation in MinIO under raw/kafka-archive/raw.gps and raw/kafka-archive/raw.trips.
6. Tune flush.size for desired object frequency and file size tradeoff.

## Key takeaway
Producer -> Kafka -> Kafka Connect -> MinIO is the correct architecture.
Your current storage behavior matches this architecture, and flush.size is the main reason files appear in chunks.