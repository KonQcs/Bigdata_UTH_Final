from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg


spark = SparkSession \
    .builder \
    .appName("Q1_DF") \
    .getOrCreate()
sc = spark.sparkContext
username = "kkiousis"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/output/Q1_df{job_id}"

tripdata2015_df = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

tripdata2015_df_up = tripdata2015_df.select("tpep_pickup_datetime", "pickup_longitude", "pickup_latitude")
filtered = tripdata2015_df_up.filter((col("pickup_longitude") != 0) & (col("pickup_latitude") != 0))
df_with_hour = filtered.withColumn("hour", hour(col("tpep_pickup_datetime")))
df_with_hour_up =  df_with_hour.select("hour", "pickup_longitude", "pickup_latitude")
grouped_df = df_with_hour_up.groupby("hour").agg(
    avg("pickup_longitude"),
    avg("pickup_latitude")
).sort(col("hour"))
result = grouped_df.withColumnRenamed("hour", "HourOfDay") \
       .withColumnRenamed("avg(pickup_longitude)", "Longitude--") \
       .withColumnRenamed("avg(pickup_latitude)", "--Latitude")

result.show()
result.coalesce(1).write.format("csv").option("header", "true").save(output_dir)
