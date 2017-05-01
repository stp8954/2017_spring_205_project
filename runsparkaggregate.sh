#! /bin/bash

while true
do
	START=`date -u --date="@$(($(date +%s) - 3600))" "+%Y-%m-%d %H:00:00"`
	END=`date -u "+%Y-%m-%d %H:00:00"`
	echo $START
	echo $END
	python ./purgedb.py
	/usr/local/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0  --py-files /home/w205/w205_project_test/geohash.py /home/w205/w205_project_test/spark_aggregate.py "$START" "$END"
	sleep 3600s
done