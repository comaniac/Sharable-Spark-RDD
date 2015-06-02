#!/bin/bash

export SRDD_HOME=/curr/cody/Course/cs249/Shareable-Spark-RDD

spark-submit --class TestApp \
	--jars ${SRDD_HOME}/target/sRDD-1.0-SNAPSHOT.jar \
	--master local[*] \
	target/testapp-0.0.0.jar
