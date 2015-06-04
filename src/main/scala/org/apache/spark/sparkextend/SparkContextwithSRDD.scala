package org.apache.spark.sparkextend

import org.apache.spark.sparkextend.ExitCode._

import scala.reflect.{ClassTag, classTag}
import scala.collection.mutable
import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.api.java._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

class SparkContextwithSRDD(config: SparkConf) extends SparkContext {
  println("SparkContextwithSRDD is created")

  def textFile(name: String, path: String, minPartitions: Int): SRDD_I[String] = {
    val code = SRDDClient.createSRDD(name, "textFile", path, minPartitions)
    code match {
      case CREATE_SUCCESS => 
        println("[SparkContextwithSRDD] Create a new SRDD: " + name)
      case CREATE_IGNORE =>
        println("[SparkContextwithSRDD] Use existed SRDD: " + name)
      case CREATE_FAILURE =>
        println("[SparkContextwithSRDD] Create SRDD failed due to unknown error: " + name)
    }
    new SRDD_I[String](name)
  }

  def objectFile[T: ClassTag](name: String, path: String, minPartitions: Int): SRDD_I[T] = {
    val code = SRDDClient.createSRDD(name, "objectFile", path, minPartitions)
    code match {
      case CREATE_SUCCESS => 
        println("[SparkContextwithSRDD] Create a new SRDD: " + name)
      case CREATE_IGNORE =>
        println("[SparkContextwithSRDD] Use existed SRDD: " + name)
      case CREATE_FAILURE =>
        println("[SparkContextwithSRDD] Create SRDD failed due to unknown error: " + name)
    }
    new SRDD_I[T](name)
  }

}

