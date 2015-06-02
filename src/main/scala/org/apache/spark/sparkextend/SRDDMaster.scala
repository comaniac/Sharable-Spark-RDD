package org.apache.spark.sparkextend

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

// comaniac: Import extended package
import org.apache.spark.sparkextend._
import org.apache.spark.sparkextend.SRDDControls._

// comaniac: Import Akka packages
import akka.actor._
import com.typesafe.config._

class SRDDMaster extends Actor {
  var conf: SparkConf = new SparkConf()
  var manager: SRDDManager = new SRDDManager(conf)

  def receive = {
    case Test(name) => 
      println("[SRDDMaster] Test command from " + name + ".")

    case ObjectFile(path, minPartitions) =>
      val rdd = manager.objectFile(path, minPartitions)
      println("[SRDDMaster] New RDD by objectFile: " + rdd)

    case TextFile(path, minPartitions) =>
      val rdd = manager.textFile(path, minPartitions)
      println("[SRDDMaster] New RDD by textFile: " + rdd)

  }
}

object SRDDMaster {
  val system = ActorSystem("SRDDMaster", ConfigFactory.load("SRDD-akka-server"))
  val actor = system.actorOf(Props[SRDDMaster], "SRDDMasterActor")
  
  def main(argStrings: Array[String]) {
    println("SRDDMaster is ready.")
  }
}

