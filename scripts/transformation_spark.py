import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType



os.environ["HADOOP_USER_NAME"] = "root"


spark = SparkSession.builder \
    .appName('starSchemaTrasnformations') \
    .master('yarn') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.30.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m") \
    .getOrCreate()

HDFS_BRONZE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/sensor_data/"
GOLD_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"

try:

    ### Silver Layer 
    df_silver = spark.read.parquet(HDFS_BRONZE_PATH)
    
    df_silver = df_silver.withColumn("event_time", F.to_timestamp("event_time"))


    #### Golden Layer
    dim_time = df_silver.select("event_time").distinct() \
        .withColumn("time_key", F.date_format("event_time", "yyyyMMddHHmmss")) \
        .withColumn("hour", F.hour("event_time")) \
        .withColumn("day", F.dayofmonth("event_time")) \
        .withColumn("month", F.month("event_time")) \
        .withColumn("year", F.year("event_time")) \
        .withColumn("day_of_week", F.dayofweek("event_time"))

    
    dim_sensor = df_silver.select("sensor_id").distinct() \
        .withColumn("sensor_type", F.lit("IoT-Agricultural-V1")) \
        .withColumn("firmware_version", F.lit("2.1.0"))

    fact_sensor_readings = df_silver \
        .withColumn("time_key", F.date_format("event_time", "yyyyMMddHHmmss")) \
        .select(
            "time_key", 
            "sensor_id", 
            "Temperature", "Humidity", "Rainfall", "pH", 
            "EC", "Solar_Radiation", "Wind_Speed", "NDVI", "EVI"
        )

    print("Writing Star Schema tables to HDFS Gold Layer...")
    
    dim_time.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_time")
    dim_sensor.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_sensor")
    fact_sensor_readings.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}fact_sensor_readings")

    print("Gold Layer (Star Schema) created successfully.")

except Exception as e:
    print(f"Transformation failed: {e}")

finally:
    spark.stop()


#### In Silver, you have a "Flat Table." In Gold, you have a Relational Schema. This is the highest level of data maturity.