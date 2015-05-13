#!/bin/bash

export SRDD_HOME=/curr/cody/Course/cs249/Shareable-Spark-RDD

if [[ $# != 1 ]]; then
    echo usage: run.sh niters
    exit 1
fi

spark-submit --class SparkKMeans \
	--jars ${SRDD_HOME}/target/sRDD-1.0-SNAPSHOT.jar \
	--master local[*] \
	target/sparkkmeans-0.0.0.jar \
	run 3 $1 converted
