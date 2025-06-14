from math import radians, sin, cos, sqrt, atan2
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg,to_timestamp,unix_timestamp, udf, max,count



spark = SparkSession \
    .builder \
    .appName("Q3_SQL_PARQ") \
    .getOrCreate()
sc = spark.sparkContext
username = "kkiousis"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/output/Q3_SQL_PARQ{job_id}"

DF_yellow_tripdata_2024 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/yellow_tripdata_2024.parquet").select("PULocationID", "DOLocationID")
DF_taxi_zone_lookup = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/taxi_zone_lookup.parquet").select("LocationID", "Borough")

DF_yellow_tripdata_2024.createOrReplaceTempView("trips")
DF_taxi_zone_lookup.createOrReplaceTempView("zones")

query = """
  SELECT
  pickup.Borough AS Borough,
  COUNT(*) AS TotalTrips
FROM trips 
JOIN zones pickup ON trips.PULocationID = pickup.LocationID
JOIN zones dropoff ON trips.DOLocationID = dropoff.LocationID
WHERE pickup.Borough NOT IN ('Unknown', 'N/A')
  AND pickup.Borough = dropoff.Borough
GROUP BY pickup.Borough
ORDER BY
  TotalTrips DESC
"""
final_taxis_sql = spark.sql(query)
final_taxis_sql.show()
final_taxis_sql.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)
