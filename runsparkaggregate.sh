#! /bin/bash

while true
do
	START=`date -u --date="@$(($(date +%s) - 3600))" "+%Y-%m-%d %H:00:00"`
	END=`date -u "+%Y-%m-%d %H:00:00"`
	echo $START
	echo $END
	python ./purgedb.py
	/usr/local/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0  --py-files ~/2017_spring_205_project/geohash.py ~/2017_spring_205_project/spark_aggregate.py "$START" "$END"
	sleep 3600s
done