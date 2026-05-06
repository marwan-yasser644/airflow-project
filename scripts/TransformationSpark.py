import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('starSchemaTrasnformations') \
    .master('yarn') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .getOrCreate()

HDFS_BRONZE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/sensor_data/"
GOLD_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"

try:
    df_silver = spark.read.parquet(HDFS_BRONZE_PATH)
    df_silver = df_silver.withColumn("event_time", F.to_timestamp("event_time"))

    # Dimension Time
    dim_time = df_silver.select("event_time").distinct() \
        .withColumn("time_key", F.date_format("event_time", "yyyyMMddHHmmss")) \
        .withColumn("hour", F.hour("event_time")) \
        .withColumn("day", F.dayofmonth("event_time")) \
        .withColumn("month", F.month("event_time")) \
        .withColumn("year", F.year("event_time"))

    # Dimension Sensor
    dim_sensor = df_silver.select("sensor_id").distinct() \
        .withColumn("sensor_type", F.lit("IoT-Agricultural-V1"))

    # Fact Table
    fact_sensor_readings = df_silver \
        .withColumn("time_key", F.date_format("event_time", "yyyyMMddHHmmss")) \
        .select("time_key", "sensor_id", "Temperature", "Humidity", "Rainfall", "pH", "EC", "Solar_Radiation", "Wind_Speed", "NDVI", "EVI")

    dim_time.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_time")
    dim_sensor.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_sensor")
    fact_sensor_readings.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}fact_sensor_readings")

    print("Gold Layer (Star Schema) created successfully.")
except Exception as e:
    print(f"Transformation failed: {e}")
finally:
    spark.stop()