from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg,to_timestamp,unix_timestamp, udf, max,count
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2

spark = SparkSession \
    .builder \
    .appName("CSV_TO_PARQUET") \
    .getOrCreate()
sc = spark.sparkContext
username    = "kkiousis"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/output/CSV_TO_PARQUET{job_id}"


df_2024 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")

df_2015 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

df_zones = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv")

df_2024.write.mode("overwrite").parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/yellow_tripdata_2024.parquet")

df_2015.write.mode("overwrite").parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/yellow_tripdata_2015.parquet")

df_zones.write.mode("overwrite").parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/taxi_zone_lookup.parquet")

