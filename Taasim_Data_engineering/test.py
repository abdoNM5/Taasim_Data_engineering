import os
from pyspark.sql import SparkSession

# 1. The Java fix (keep this!)
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"

# 2. THE NEW FIX: Tell Spark exactly which Python to use for its workers
# Make sure this path points to your actual python.exe
python_path = r"C:\Users\nmira\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

print("1. Attempting to initialize Spark engine...")
try:
    spark = SparkSession.builder \
        .appName("Spark-Sanity-Check") \
        .master("local[*]") \
        .getOrCreate()
    print("2. SUCCESS: Spark Session Created!")

    print("3. Attempting to build a tiny DataFrame...")
    data = [("Alice", 1), ("Bob", 2), ("Casablanca", 3)]
    df = spark.createDataFrame(data, ["Name", "Value"])
    print("4. SUCCESS: DataFrame built in memory!")

    print("5. Attempting to execute an action (counting)...")
    count = df.count()
    print(f"6. SUCCESS! Counted {count} rows. Spark is fully operational.")

except Exception as e:
    print(f"\n❌ SPARK FAILED. Error details:\n{e}")
    
finally:
    if 'spark' in locals():
        spark.stop()
        print("7. Spark Session Closed.")