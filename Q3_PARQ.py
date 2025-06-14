from math import radians, sin, cos, sqrt, atan2
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg,to_timestamp,unix_timestamp, udf, max,count


spark = SparkSession \
    .builder \
    .appName("Q3_PARQ") \
    .getOrCreate()
sc = spark.sparkContext
username = "kkiousis"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/output/Q3_PARQ{job_id}"

DF_yellow_tripdata_2024 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/yellow_tripdata_2024.parquet").select("PULocationID", "DOLocationID")
DF_taxi_zone_lookup = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/taxi_zone_lookup.parquet").select("LocationID", "Borough")

pickup_join = DF_yellow_tripdata_2024.join(DF_taxi_zone_lookup.withColumnRenamed("LocationID", "pu_locid").withColumnRenamed("Borough", "pickup_borough"), DF_yellow_tripdata_2024["PULocationID"] == col("pu_locid"))


drop_join = pickup_join.join(DF_taxi_zone_lookup.withColumnRenamed("LocationID", "do_locid").withColumnRenamed("Borough", "dropoff_borough"), pickup_join["DOLocationID"] == col("do_locid"))


same_trips = drop_join.filter((col("pickup_borough") == col("dropoff_borough")) & ((col("pickup_borough") != 'Unknown') & (col("pickup_borough") != 'N/A')))
final_trips = same_trips.groupBy("pickup_borough").agg(count("*").alias("TotalTrips")).orderBy(col("TotalTrips").desc()).withColumnRenamed("pickup_borough", "Borough")

final_trips.show()
final_trips.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)
