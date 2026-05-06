import os
from pyspark.sql import SparkSession

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('LoadToSnowflake') \
    .master('yarn') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3,net.snowflake:snowflake-jdbc:3.13.22") \
    .getOrCreate()

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
    df = spark.read.parquet(f"{GOLD_BASE_PATH}{table_name}")
    df.write.format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", table_name.upper()) \
        .mode("overwrite") \
        .save()
    print(f"{table_name} loaded successfully!")

try:
    load_to_snowflake("dim_time")
    load_to_snowflake("dim_sensor")
    load_to_snowflake("fact_sensor_readings")
except Exception as e:
    print(f"Loading failed: {e}")
finally:
    spark.stop()