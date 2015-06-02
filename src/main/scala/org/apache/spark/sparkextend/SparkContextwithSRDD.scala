package org.apache.spark.sparkextend

import scala.reflect.{ClassTag, classTag}
import scala.collection.mutable
import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.api.java._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

class SparkContextwithSRDD(config: SparkConf) extends SparkContext {
  println("SparkContextwithSRDD is created")

  def textFileSRDD(name: String, path: String, minPartitions: Int = defaultMinPartitions): SRDD_I = {
    val result = SRDDClient.createSRDD(name, "textFile", path, minPartitions)
    if (result == 1)
      println("[SparkContextwithSRDD] Create a new RDD: " + name)
    else
      println("[SparkContextwithSRDD] Use existed RDD: " + name)
    new SRDD_I(name)
  }
}

