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

  val sRDDMap = mutable.HashMap[String, sRDD[_]]()

  def bindsRDD[T: ClassTag](rdd: sRDD[T]) = {
    sRDDMap(rdd.UniqueName) = rdd
    println("sRDD " + rdd.UniqueName + " is binded.")
  }

  def listsRDD() = {
    println("List recordrd sRDD:")
    sRDDMap.keys.foreach{
      key => 
      println("UniqueName: " + key)
    }
  }

}

