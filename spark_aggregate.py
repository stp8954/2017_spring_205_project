import pyspark 
import sys 
import json 
import datetime 
import geohash as gh 
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *


def decode_hash(hash):
	loc = gh.decode_exactly(hash)
	return loc


startdt , enddt = sys.argv[1:]

fmt = '%Y-%m-%d %H:%M:%S'
start = datetime.strptime(startdt,fmt)
end = datetime.strptime(enddt,fmt)

sqlfunc = udf(decode_hash)

spark_session = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bus_locationdb.location_collection_raw") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/bus_locationdb.bus_aggregate") \
    .getOrCreate()


df = spark_session.read.format("com.mongodb.spark.sql.DefaultSource").load()

hashlen = (5,6,7,8)

for l in hashlen:
	key = "geohash" + str(l)
	gdata = spark_session.createDataFrame(df.filter((df['datetime'] >= start) & (df['datetime'] <= end)).groupBy(key).count().collect())
	gt = gdata.withColumn('aggtime',lit(start)).withColumn('loc',sqlfunc(key)).withColumn('hashlen',lit(l))
	gt.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","bus_locationdb").option("collection", "bus_aggregate").save()