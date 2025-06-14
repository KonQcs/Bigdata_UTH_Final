from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, count, avg, to_timestamp

spark = SparkSession \
    .builder \
    .appName("Q6_2x4x8") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

sc = spark.sparkContext
username = "kkiousis"
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/output/Q6_2x4x8_{job_id}"

df = spark.read.option("header", "true").csv(f"hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")


df = df.withColumn("trip_distance", col("trip_distance").cast("double")) \
       .withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))

df = df.select("tpep_pickup_datetime", "trip_distance") \
       .filter((col("trip_distance") > 0) & (col("tpep_pickup_datetime").isNotNull())) \
       .withColumn("month", month(col("tpep_pickup_datetime")))

result = df.groupBy("month").agg(
    count("*").alias("TotalTrips"),
    avg("trip_distance").alias("AvgDistance")
).orderBy("month")

result.show()

result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
