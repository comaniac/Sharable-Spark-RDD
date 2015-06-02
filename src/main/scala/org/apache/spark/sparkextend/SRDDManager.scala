package org.apache.spark.sparkextend

import scala.reflect.{ClassTag, classTag}
import scala.collection.mutable
import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.api.java._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

class SRDDManager(config: SparkConf) extends SparkContext {
  println("SRDDManager is created")

  val SRDDMap = mutable.HashMap[String, SRDD[_]]()

  def bindSRDD[T: ClassTag](rdd: SRDD[T]) = {
    SRDDMap(rdd.UniqueName) = rdd
    println("SRDD " + rdd.UniqueName + " is binded.")
  }

  def getSRDD[T: ClassTag](name: String): SRDD[T] = {
    try {
      SRDDMap(name).asInstanceOf[SRDD[T]]
    } catch {
      case e: Exception => println("Fail to retrieve SRDD " + name + ": " + e)
      null
    }
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

object SRDDManager {
  var sc: SRDDManager = null
  
  def getSC(config: SparkConf): SRDDManager = {
    if (sc == null)
      sc = new SRDDManager(config)
    sc
  }

}
