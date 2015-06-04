# Shareable-Spark-RDD
This project aims to create an extension package of Spark to support shareable RDDs between Spark contexts.

Requirement:
	- Spark executable. (1.2.1 or above)
	- JDK 7. (environment variable JAVA_HOME must be set)
	- Environment variable SRDD_HOME must be set.

Install:
	- ./$SRDD_HOME/build/mvn package

How to Use:
	- Import package
		- import org.apache.spark.sparkextend._
	- Add dependency to Maven configure pom.xml
	- mvn package

Development Progress:

1. Record SRDD list to the SparkContext. (DONE)
	- User needs to specify the name of sRDD explicitly.
	- The name of RDDs must be unique.
	- The SRDD can be shared if user uses the unique name of SRDD to retrieve it.

2. Make sRDD shareable (can be accessed by others). (DONE)
	- Create a special SparkContext when launching Spark.
	- SRDD is created under the special SparkContext.
	- Simple garbage collection.

3. Integrate to Spark source

4. Auto match the same sRDDs.

5. Intelligent garbage collection.

