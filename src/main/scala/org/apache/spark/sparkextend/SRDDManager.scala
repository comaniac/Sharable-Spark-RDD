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
  println("SRDDManager is created, GC threshold is set to " + GCThld(0) + ", " + GCThld(1))

  val SRDDMap = mutable.HashMap[String, SRDD[_]]()
  val TrashSRDD = mutable.HashMap[String, SRDD[_]]()

  def bindSRDD[T: ClassTag](rdd: SRDD[T]) = {
    val srdd: Option[SRDD[T]] = getSRDD(rdd.UniqueName)
    if (!srdd.isDefined) {
      SRDDMap(rdd.UniqueName) = rdd
      println("[SRDDManager] SRDD " + rdd.UniqueName + " is binded.")
    } 
    else
      println("[SRDDManager] SRDD " + rdd.UniqueName + " is rebinded.")
  }

  def getSRDD[T: ClassTag](name: String): Option[SRDD[T]] = {
    var srdd: SRDD[T] = null
    if (hasSRDD(name))
      srdd = SRDDMap(name).asInstanceOf[SRDD[T]]
    else if (SRDDUtil.isRoot(name) && TrashSRDD.exists(_._1 == name)) {
      srdd = TrashSRDD(name).asInstanceOf[SRDD[T]]
      SRDDMap(name) = srdd
      TrashSRDD -= name
    }
    if (srdd != null) {
      srdd.use
      Some(srdd)
    } 
    else
      None
  }

  def hasSRDD(name: String) = {
    if (SRDDMap.exists(_._1 == name))
      true
    else
      false
  }

  def touchSRDD[T: ClassTag](name: String) = {
    if (SRDDMap.exists(_._1 == name)) {
      val srdd: SRDD[T] = SRDDMap(name).asInstanceOf[SRDD[T]]
      srdd.use
    }
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
        if (SRDDUtil.isRoot(srdd.UniqueName)) {
          TrashSRDD(srdd.UniqueName) = srdd
          println("Remove unused SRDD to trash " + key)
        }
        else
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
