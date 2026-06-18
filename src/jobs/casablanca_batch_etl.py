from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import ArrayType, DoubleType, StructType, StructField, StringType, LongType
import json
import h3

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CasablancaBatchETL") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Path to existing data
input_path = "storage/data/raw/casablanca_real_roads_final.csv"
output_path = "s3a://curated/trips/"

# Define UDF to parse polyline and extract points
def parse_polyline(polyline_str):
    try:
        points = json.loads(polyline_str)
        return points
    except:
        return []

parse_polyline_udf = udf(parse_polyline, ArrayType(ArrayType(DoubleType())))

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
df_transformed = df_raw.withColumn("coords", parse_polyline_udf(col("CASA_POLYLINE"))) \
    .filter("size(coords) > 0") \
    .withColumn("pickup_lon", col("coords")[0][0]) \
    .withColumn("pickup_lat", col("coords")[0][1]) \
    .withColumn("dropoff_lon", col("coords").getItem(col("size(coords)") - 1)[0]) \
    .withColumn("dropoff_lat", col("coords").getItem(col("size(coords)") - 1)[1])

# 3. Add Geospatial Index (H3)
df_curated = df_transformed.withColumn("pickup_h3", h3_udf(col("pickup_lon"), col("pickup_lat"))) \
    .withColumn("dropoff_h3", h3_udf(col("dropoff_lon"), col("dropoff_lat"))) \
    .drop("coords", "CASA_POLYLINE")

# 4. Save to MinIO as Parquet
df_curated.write.mode("overwrite").parquet(output_path)

print(f"ETL Complete. Curated data saved to {output_path}")
spark.stop()
