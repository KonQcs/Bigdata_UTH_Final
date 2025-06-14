from pyspark.sql import SparkSession
from math import radians, sin, cos, sqrt, atan2,asin
from datetime import datetime



sc = SparkSession \
    .builder \
    .appName("Q2_RDD") \
    .getOrCreate() \
    .sparkContext
username = "kkiousis"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/output/Q2_RDD{job_id}"




def max(x1, x2):
    return x1 if x1[0] > x2[0] else x2

def haversine_dist(x):
    try:
        pickup_time = datetime.strptime(x[1], "%Y-%m-%d %H:%M:%S")
        dropoff_time = datetime.strptime(x[2], "%Y-%m-%d %H:%M:%S")
        lon1, lat1, lon2, lat2 = map(radians, [float(x[3]), float(x[4]), float(x[5]), float(x[6])])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        r = 6371
        dist = c * r
        time= (dropoff_time - pickup_time).total_seconds() / 60
        if dist > 50 or time < 10 or (dist / time > 120.0):
            return 0, 0
        return dist, time
    except:
        return 0, 0
raw_rdd = sc.textFile("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")
header = raw_rdd.first()

final_rdd = raw_rdd.filter(lambda line: line != header).map(lambda x: x.split(",")) \
    .filter(lambda x: all([len(x) > 10,x[5] != '0', x[6] != '0', x[9] != '0', x[10] != '0',-90 <= float(x[6]) <= 90, -180 <= float(x[5]) <= 180,-90 <= float(x[10]) <= 90, -180 <= float(x[9]) <= 180," " in x[1], " " in x[2]])).map(lambda x: (x[0], haversine_dist([x[0], x[1], x[2], x[5], x[6], x[9], x[10]]))).reduceByKey(max)

print("| VendorID |\t--| Max Haversine Dist(km) |\t--| Duration(min) |")
for vendor , (dist, time) in final_rdd.collect():
    print(f"\t{vendor}\t\t\t{dist:.2f}\t\t\t\t\t{time:.1f}")
final_rdd.coalesce(1).saveAsTextFile(output_dir)
