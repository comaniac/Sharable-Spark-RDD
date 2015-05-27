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

  val SRDDMap = mutable.HashMap[String, SRDD[_]]()

  def bindSRDD[T: ClassTag](rdd: SRDD[T]) = {
    SRDDMap(rdd.UniqueName) = rdd
    println("SRDD " + rdd.UniqueName + " is binded.")
  }

  def getSRDD[T: ClassTag](name: String) = {
    if (SRDDMap.exists(_._1 == name))
//      SRDDMap(name)
      null
    else
      null
  }

  def hasSRDD(name: String) = {
    if (SRDDMap.exists(_._1 == name))
      true
    else
      false
  }

  def listSRDD() = {
    println("List recordrd SRDD:")
    SRDDMap.keys.foreach{
      key => 
      println("UniqueName: " + key)
    }
  }

}

