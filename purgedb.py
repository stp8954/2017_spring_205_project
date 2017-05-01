#!/usr/bin/env python
import json
from pymongo import MongoClient 
from datetime import datetime
import sys

#this script runs before the spark aggregator runs.
# it deletes the data for the hour to be processed in case the SparkAggregator job get kicked off multile times in the same houe
key = datetime.utcnow().strftime('%Y-%m-%d %H:00:00')
client = MongoClient('mongodb://127.0.0.1:27017')
db = client.bus_locationdb
collection = db.bus_aggregate
collection.delete_many({"aggtime": datetime.strptime(key,'%Y-%m-%d %H:00:00')})
