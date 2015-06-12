package org.apache.spark.sparkextend

import scala.reflect.{ClassTag, classTag}
import scala.collection.mutable
import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.api.java._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

class SRDDManager(config: SparkConf) extends SparkContext {
  val GCThld = Array(-3, -5)
  println("SRDDManager is created, GC threshold is set to " + GCThld)

  val SRDDMap = mutable.HashMap[String, SRDD[_]]()

  def bindSRDD[T: ClassTag](rdd: SRDD[T]) = {
    SRDDMap(rdd.UniqueName) = rdd
    println("[SRDDManager] SRDD " + rdd.UniqueName + " is binded.")
  }

  def getSRDD[T: ClassTag](name: String): SRDD[T] = {
    try {
      val srdd: SRDD[T] = SRDDMap(name).asInstanceOf[SRDD[T]]
      srdd.use
      srdd
    } catch {
      case e: Exception => println("[SRDDManager] Fail to retrieve SRDD " + name + ": " + e)
      null
    }
  }

  def hasSRDD(name: String) = {
    if (SRDDMap.exists(_._1 == name))
      true
    else
      false
  }

  def deleteSRDD(name: String) = {
    if (SRDDMap.exists(_._1 == name))
      SRDDMap -= name
  }

  def listSRDD() = {
    println("[SRDDManager] List recordrd SRDD:")
    SRDDMap.keys.foreach{
      key => 
      println("UniqueName: " + key + " Freq: " + SRDDMap(key).freq)
    }
  }

  def gc() = {
    println("[SRDDManager] === GC start ===")
    SRDDMap.keys.foreach{
      key => 
      val srdd = SRDDMap(key)
      if (srdd.freq < GCThld(1)) {
        println("Remove unused SRDD " + key)
        SRDDMap -= key
      }
      else if (srdd.freq == GCThld(0)) {
        println("Keep SRDD " + key + " in disk with frequent counter " + srdd.freq)
        srdd.unpersist()
        srdd.unuse
      }
      else {
        println("Keep SRDD " + key + " with frequent counter " + srdd.freq)
        srdd.unuse
      }
    }
    println("[SRDDManager] === GC end ===")
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
