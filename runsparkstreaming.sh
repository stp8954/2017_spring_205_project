#! /bin/bash

/usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1  --py-files ./geohash.py  ./spark_transform.py