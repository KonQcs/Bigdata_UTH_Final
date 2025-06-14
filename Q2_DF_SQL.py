from math import radians, sin, cos, sqrt, atan2,asin
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg,to_timestamp,unix_timestamp, udf, max


spark = SparkSession \
    .builder \
    .appName("Q2_DF_SQL") \
    .getOrCreate()
sc = spark.sparkContext
username = "kkiousis"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/output/Q2_DF_SQL{job_id}"


tripdata2015_df = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

tripdata2015_df_up = tripdata2015_df.select("VendorID", "tpep_dropoff_datetime", "tpep_pickup_datetime", "pickup_longitude", "pickup_latitude","dropoff_longitude", "dropoff_latitude")
filtered = tripdata2015_df_up.filter((col("pickup_longitude") != 0) & (col("pickup_latitude") != 0) & (col("dropoff_longitude") != 0) & (col("dropoff_latitude") != 0))

filtered.createOrReplaceTempView("trips")
def haversine(lon1, lat1, lon2, lat2):
    R = 6371
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))

spark.udf.register("haversine", haversine, FloatType())
query1 = """
SELECT
    VendorID, tpep_pickup_datetime,tpep_dropoff_datetime,
    haversine(pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude) AS distance,
    (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60 AS time
FROM trips 
"""


tripdata2015_df = spark.sql(query1)
tripdata2015_df_up = tripdata2015_df.select("VendorID", "distance", "time").filter((col("distance") > 0) & (col("distance") < 50) & (col("time") > 0) & (col("time") < 1000))
tripdata2015_df_up.createOrReplaceTempView("final_result")

query2 = """
SELECT VendorID, distance AS MaxHaversineDistanse, time AS Duration
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY VendorID ORDER BY distance DESC) AS row_num
  FROM final_result 
) AS ranked
WHERE row_num = 1
ORDER BY VendorID
"""
result = spark.sql(query2)

final_result = result.select(col("VendorID"), (col("MaxHaversineDistanse").alias(" |-| Max Haversine Distanse(km) |")), (col("Duration").alias("-| Duration(min) |")))

final_result.show()

final_result.coalesce(1).write.format("csv").option("header", "true").save(output_dir)
