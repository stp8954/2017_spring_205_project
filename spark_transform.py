import pyspark 
import sys 
import json 
import datetime 
from pyspark.streaming import StreamingContext 
from pyspark.streaming.kafka import KafkaUtils , TopicAndPartition 
from pyspark import SparkContext, SparkConf 
from pyspark.sql import SparkSession 
from pymongo import MongoClient 
import json 
import geohash as gh 
  

APP_NAME = 'Spark_Kafka_To_MongoDB_2' 
TOPICS = ['raw_loc_data'] 

# location for storing spark checkpoints
CHECKPOINT = '/tmp/%s' % APP_NAME 

#kafka params
KAFKA_PARAMS = { 
   'metadata.broker.list' : 'localhost:9092'
} 

STREAM_CONTEXT_TIMEOUT = 60  

def quiet_logs( sc ): 
     logger = sc._jvm.org.apache.log4j 
     logger.LogManager.getLogger("org").setLevel( logger.Level.ERROR ) 

#calculate geohash for the given location
def calc_geohash(lat,lon , length): 
    return gh.encode(lat,lon,length) 


#convert the data received from kafka into MongoDB document format
def convertToMongoRecord(loc_data): 
    d = json.loads(loc_data) 
    lat = float(d['latitude']) 
    lon = float(d['longitude']) 
    heading = float(d['heading']) 

    post = {'bus_id' : d['id'], 
    'route_tag': d['route_tag'], 
    'direction_tag': d['direction_tag'], 
    'loc' : {'type': 'Point', 'coordinates':[lat,lon]}, 
    'heading': heading, 
    'geohash' : calc_geohash(lat,lon,7), 
    'datetime': d['timestamp']} 
    collection.insert(post)

    return post 

count =0
#convert the data received from kafka into MongoDB document format
#Since this function is called in the foreachPartition function, spark tries to serialize it for distributed processing
# For that reason the client connection has to be created in the function
def convertToMongoRecord2(itr): 
	client = MongoClient('mongodb://127.0.0.1:27017') 
	db = client.bus_locationdb 
	collection = db.location_collection_raw 

	for loc_data in itr:
		d = json.loads(loc_data[1]) 
		lat = float(d['latitude']) 
		lon = float(d['longitude']) 
		heading = float(d['heading']) 
		post = {'bus_id' : d['id'], 
		'route_tag': d['route_tag'], 'direction_tag': d['direction_tag'], 
        'loc' : {'type': 'Point', 'coordinates':[lat,lon]}, 
        'heading': heading, 
        'geohash5' : calc_geohash(lat,lon,5), 
        'geohash6' : calc_geohash(lat,lon,6),
        'geohash7' : calc_geohash(lat,lon,7),
        'geohash8' : calc_geohash(lat,lon,8),
        'datetime': d['timestamp']} 
		collection.insert(post)
		count + count + 1
		if (count %10 == 0):
			print(post)


#this method tries to create a context from the checkpoint first if it exists.
#This allows Spark to continue form where it last left off
# if the checkpoint doesnt exits, it creates a new Context
def create_context(): 
    spark_conf = SparkConf().setMaster('local[2]').setAppName(APP_NAME) 
    sc = pyspark.SparkContext(conf= spark_conf)
    sc.setLogLevel('WARN')
    ssc = StreamingContext(sc, 5) 
    ssc.checkpoint(CHECKPOINT) 

    # start offsets from beginning 
    # won't work if we have a chackpoint 
    offsets = {TopicAndPartition(topic, 0): 0 for topic in TOPICS} 
    stream = KafkaUtils.createDirectStream(ssc, TOPICS, KAFKA_PARAMS, offsets) 
    process(stream) 
    return ssc 

def process(stream):
    stream.foreachRDD(lambda rdd: rdd.foreachPartition(convertToMongoRecord2))
  

#Create StreamingContext
ssc = StreamingContext.getOrCreate(CHECKPOINT, create_context) 
ssc.start() 
ssc.awaitTermination() 