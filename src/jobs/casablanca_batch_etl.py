from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, size
from pyspark.sql.types import ArrayType, DoubleType, StructType, StructField, StringType, LongType
import json
import h3

import os
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CasablancaBatchETL") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Path to existing data
input_path = "storage/data/raw/casablanca_real_roads_final.csv"
output_path = "s3a://curated/trips/"

from pyspark.sql.functions import from_json
# Define schema for the polyline JSON array
polyline_schema = ArrayType(ArrayType(DoubleType()))

# Define UDF to compute H3 cell (Resolution 8)
def get_h3_cell(lon, lat):
    try:
        return h3.geo_to_h3(lat, lon, 8)
    except:
        return None

h3_udf = udf(get_h3_cell, StringType())

# 1. Read Raw CSV Data
df_raw = spark.read.option("header", "true").csv(input_path)

# 2. Transform Data
# Parse polyline and extract start/end points
df_transformed = df_raw.withColumn("coords", from_json(col("CASA_POLYLINE"), polyline_schema)) \
    .filter(size(col("coords")) > 0) \
    .withColumn("pickup_lon", col("coords")[0][0]) \
    .withColumn("pickup_lat", col("coords")[0][1]) \
    .withColumn("dropoff_lon", col("coords").getItem(size(col("coords")) - 1)[0]) \
    .withColumn("dropoff_lat", col("coords").getItem(size(col("coords")) - 1)[1])

# 3. Add Geospatial Index (H3)
df_curated = df_transformed.withColumn("pickup_h3", h3_udf(col("pickup_lon"), col("pickup_lat"))) \
    .withColumn("dropoff_h3", h3_udf(col("dropoff_lon"), col("dropoff_lat"))) \
    .drop("coords", "CASA_POLYLINE")

# 4. Save to MinIO as Parquet
df_curated.write.mode("overwrite").parquet(output_path)

print(f"ETL Complete. Curated data saved to {output_path}")
spark.stop()
