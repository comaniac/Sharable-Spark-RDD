# Shareable-Spark-RDD
This project aims to create an extension package of Spark to support shareable RDDs between Spark contexts.

Progress:

1. Record sRDDs to the file while creating. (NOW WORKING)
	- User needs to specify the name of sRDD explicitly.
	- The name of RDDs must be unique.

2. Make sRDD shareable (can be accessed by others).
	- We may probably register the record file to Spark file management.
	- Live in memory / Live in disk / Dead in disk.

3. Auto match the same sRDDs.

4. Intelligent garbage collection.

