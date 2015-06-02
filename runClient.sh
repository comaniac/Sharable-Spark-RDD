#!/bin/bash

export SRDD_HOME=/curr/cody/Course/cs249/Shareable-Spark-RDD

spark-submit --class org.apache.spark.sparkextend.SRDDClient \
	--master local[*] \
	${SRDD_HOME}/target/sRDD-1.0-SNAPSHOT.jar \
	$1
