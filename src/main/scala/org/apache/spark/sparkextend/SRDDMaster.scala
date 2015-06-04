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
import org.apache.spark.sparkextend.ExitCode._

// comaniac: Import Akka packages
import akka.event._
import akka.actor._
import akka.dispatch._
import akka.pattern._
import akka.util._
import com.typesafe.config._

class SRDDMaster extends Actor with ActorLogging {
  var conf: SparkConf = new SparkConf()
  var manager: SRDDManager = new SRDDManager(conf)

  def receive = {
    case Test(name) => 
      println("[SRDDMaster] Test command from " + name + ".")

    case ObjectFile(name, path, minPartitions) =>
      val result = SRDDWrapper.wrap(name, manager, manager.objectFile(path, minPartitions))
      println("[SRDDMaster] objectFile done (" + result + ").")
      sender ! result

    case TextFile(name, path, minPartitions) =>
      val result = SRDDWrapper.wrap(name, manager, manager.textFile(path, minPartitions))
      println("[SRDDMaster] textFile done (" + result + ").")
      sender ! result

    case Count(name) =>
      val srdd = manager.getSRDD(name)
      println("[SRDDMaster] count done.")
      sender ! srdd.rdd.count

    case Collect(name) =>
      var srdd = manager.getSRDD(name)
      println("[SRDDMaster] collect done.")
      sender ! srdd.rdd.collect

    case Cache(name) =>
      val srdd = manager.getSRDD(name)
      srdd.cache
      println("[SRDDMaster] cache done.")
      sender ! EXIT_SUCCESS

    case MapPow(name, p) =>
      val srdd = manager.getSRDD(name).asInstanceOf[SRDD[String]]
      if (!manager.hasSRDD(name + "_pow" + p))
        SRDDWrapper.wrap(name + "_pow" + p, manager, srdd.rdd.map(e => (pow(e.toDouble, p.toDouble)).toString))
      else
        println("[SRDDMaster] Use existed SRDD " + name + "_pow" + p)
      println("[SRDDMaster] map.pow done.")
      sender ! (name + "_pow" + p)

    case ReduceSum(name) =>
      val srdd = manager.getSRDD(name).asInstanceOf[SRDD[String]]
      val result = srdd.rdd.map(e => e.toDouble).reduce((a, b) => (a + b))
      println("[SRDDMaster] reduce.sum done.")
      sender ! result.toString

    case _ =>
      println("[SRDDMaster] Unknown message.")
      sender ! EXIT_FAILURE
  }
}

object SRDDMaster {
  val system = ActorSystem("SRDDMaster", ConfigFactory.load("SRDD-akka-server"))
  val actor = system.actorOf(Props[SRDDMaster], "SRDDMasterActor")
  
  def main(argStrings: Array[String]) {
    println("SRDDMaster is ready.")
  }
}

