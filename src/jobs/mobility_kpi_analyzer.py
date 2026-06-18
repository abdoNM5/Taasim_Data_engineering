from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, hour, from_unixtime, to_date, lit
import time

# Initialize Spark Session with Cassandra support
spark = SparkSession.builder \
    .appName("MobilityKPIAnalyzer") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .getOrCreate()

# Path to curated data in MinIO
input_path = "s3a://curated/trips/"

# 1. Read Curated Parquet Data
df_curated = spark.read.parquet(input_path)

# 2. Compute KPIs
# KPI 1: Trips per H3 Zone (Resolution 8)
df_zone_demand = df_curated.groupBy("pickup_h3").count() \
    .withColumnRenamed("count", "total_trips")

# KPI 2: Peak Demand Hours
df_peak_hours = df_curated.withColumn("hour", hour(from_unixtime(col("TIMESTAMP").cast("long")))) \
    .groupBy("hour").count() \
    .orderBy("count", ascending=False)

# 3. Format result for Cassandra (demand_zones table)
# city, zone_id (using hash of H3 or similar), window_start, active_vehicles, pending_requests, ratio
# Mocking some fields as this is a batch aggregation
cassandra_output = df_zone_demand.withColumn("city", lit("Casablanca")) \
    .withColumn("zone_id", col("pickup_h3").cast("int")) \
    .withColumn("window_start", lit(int(time.time()))) \
    .withColumn("active_vehicles", col("total_trips")) \
    .withColumn("pending_requests", lit(0)) \
    .withColumn("ratio", lit(1.0)) \
    .select("city", "zone_id", "window_start", "active_vehicles", "pending_requests", "ratio")

# 4. Write to Cassandra
try:
    cassandra_output.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="demand_zones", keyspace="taasim") \
        .mode("append") \
        .save()
    print("KPIs successfully loaded into Cassandra.")
except Exception as e:
    print(f"Warning: Cassandra write failed (check connection or schema): {e}")

print("Analytics Job Complete.")
spark.stop()
