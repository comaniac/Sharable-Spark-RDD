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

  override def textFile(path: String, minPartitions: Int = defaultMinPartitions): SRDD[String] = {
    SRDDClient.createSRDD("textFile", path, minPartitions)

    null // FIXME
  }
}

