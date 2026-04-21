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
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3,net.snowflake:snowflake-jdbc:3.13.22") \
    .getOrCreate()


# 3. Snowflake Connection Options
sf_options = {
    "sfURL": "LLYHYMM-YJ95431.snowflakecomputing.com",
    "sfUser": "YOURUSERNAME",
    "sfPassword": "YOURPASSWORD",
    "sfDatabase": "AGRI_DATA_DB",
    "sfSchema": "GOLD_LAYER",
    "sfWarehouse": "AGRI_WH"
}

GOLD_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"

def load_to_snowflake(table_name):
    print(f"Reading {table_name} from HDFS...")
    df = spark.read.parquet(f"{GOLD_BASE_PATH}{table_name}")
    
    print(f"Loading {table_name} to Snowflake...")
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", table_name.upper()) \
        .mode("overwrite") \
        .save()
    print(f"{table_name} loaded successfully!")

# 4. Execute the Load for each table in the Star Schema
try:
    load_to_snowflake("dim_time")
    load_to_snowflake("dim_sensor")
    load_to_snowflake("fact_sensor_readings")
    print("ALL GOLD TABLES LOADED TO SNOWFLAKE")
except Exception as e:
    print(f" Loading failed: {e}")
finally:
    spark.stop()