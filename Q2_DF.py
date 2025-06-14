from math import radians, sin, cos, sqrt, atan2
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg,to_timestamp,unix_timestamp, udf, max


spark = SparkSession \
    .builder \
    .appName("Q2_df") \
    .getOrCreate()
sc = spark.sparkContext
username = "kkiousis"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/output/Q2_DF{job_id}"

tripdata2015_df = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

tripdata2015_df_up = tripdata2015_df.select("VendorID", "tpep_dropoff_datetime", "tpep_pickup_datetime", "pickup_longitude", "pickup_latitude","dropoff_longitude", "dropoff_latitude")
filtered = tripdata2015_df_up.filter((col("pickup_longitude") != 0) & (col("pickup_latitude") != 0) & (col("dropoff_longitude") != 0) & (col("dropoff_latitude") != 0))
filtered = filtered.withColumn("pickup_ts", to_timestamp("tpep_pickup_datetime")).withColumn("dropoff_ts", to_timestamp("tpep_dropoff_datetime"))
filtered_time = filtered.withColumn("Duration(min)", (unix_timestamp("dropoff_ts") - unix_timestamp("pickup_ts")) / 60)

def haversine_udf(lon1, lat1, lon2, lat2):
    R = 6371
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return float(R * 2 * atan2(sqrt(a), sqrt(1 - a)))

haversin_km = udf(haversine_udf, FloatType())
filtered_diad = filtered_time.withColumn("distance", haversin_km(col("pickup_longitude"), col("pickup_latitude"),col("dropoff_longitude"), col("dropoff_latitude"))).filter((col("distance") < 50) & (col("Duration(min)") > 10) )

max_km = filtered_diad.groupBy(col("VendorID").alias("Vendor")).agg(max("distance").alias("Max Haversine DIstance(km)"))

result = filtered_diad.join(max_km, (filtered_diad["VendorID"] == max_km["Vendor"]) & (filtered_diad["distance"] == max_km["Max Haversine DIstance(km)"]))

final_result = result.select(col("VendorId"), col("Max Haversine DIstance(km)"), col("Duration(min)"))
final_result.show()

final_result.coalesce(1).write.format("csv").option("header", "true").save(output_dir)
