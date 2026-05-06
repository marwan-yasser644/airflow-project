import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('AirFlowBatchProcessingJop') \
    .master('yarn') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.driver.host", "172.30.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("Rainfall", DoubleType(), True),
    StructField("pH", DoubleType(), True),
    StructField("EC", DoubleType(), True),
    StructField("Solar_Radiation", DoubleType(), True),
    StructField("Wind_Speed", DoubleType(), True),
    StructField("NDVI", DoubleType(), True),
    StructField("EVI", DoubleType(), True)
])

input_path = "file:///C:/Users/it shop/OneDrive/Desktop/project/data/raw_sensor_pings/"

try:
    raw_df = spark.read.schema(schema).json(input_path)
    record_count = raw_df.count()

    if record_count > 0:
        hdfs_output_path = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/sensor_data/"
        raw_df.write.mode("append").format("parquet").save(hdfs_output_path)
        print(f"Batch ingestion complete. {record_count} records saved to Bronze.")
    else:
        print("No new data found.")
except Exception as e:
    print(f"Error: {e}")
finally:
    spark.stop()