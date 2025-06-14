from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg,to_timestamp,unix_timestamp, udf, max,count


spark = SparkSession \
    .builder \
    .appName("Q5_PARQ") \
    .getOrCreate()
    
sc = spark.sparkContext
username = "kkiousis"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/output/Q5_PARQ_{job_id}"

tripdata2024 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/yellow_tripdata_2024.parquet").select("PULocationID", "DOLocationID")
zone_lookup = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/taxi_zone_lookup.parquet").select("LocationID", "Borough", "Zone")


pickup_join = tripdata2024.join(zone_lookup.withColumnRenamed("LocationID", "pu_locid").withColumnRenamed("Borough", "pickup_borough").withColumnRenamed("Zone", "Pickup Zone"), tripdata2024["PULocationID"] == col("pu_locid"))


drop_join = pickup_join.join(zone_lookup.withColumnRenamed("LocationID", "do_locid").withColumnRenamed("Borough", "dropoff_borough").withColumnRenamed("Zone", "Dropoff Zone"), pickup_join["DOLocationID"] == col("do_locid"))

same_trips = drop_join.filter((col("Pickup Zone") != col("Dropoff Zone")) & ((col("pickup_borough") != 'Unknown') & (col("pickup_borough") != 'N/A')))
result = same_trips.groupBy("Pickup Zone", "Dropoff Zone").count().withColumnRenamed("count", "TotalTrips").sort(col("TotalTrips").desc())

result.show(4)
result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)