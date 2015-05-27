# Shareable-Spark-RDD
This project aims to create an extension package of Spark to support shareable RDDs between Spark contexts.

Progress:

1. Record SRDD list to the SparkContext. (DONE)
	- User needs to specify the name of sRDD explicitly.
	- The name of RDDs must be unique.
	- The SRDD can be shared if user uses the unique name of SRDD to retrieve it.

2. Make sRDD shareable (can be accessed by others). (NOW WORKING)
	- Create a special SparkContext when launching Spark.
	- SRDD is created under the special SparkContext.
	- Simple garbage collection.

3. Auto match the same sRDDs.

4. Intelligent garbage collection.

