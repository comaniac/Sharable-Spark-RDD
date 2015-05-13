#!/bin/bash

spark-submit --class SparkKMeans \
	--master local[*] \
	target/sparkkmeans-0.0.0.jar \
	 convert \
   input converted
