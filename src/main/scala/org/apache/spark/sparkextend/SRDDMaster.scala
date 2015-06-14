package org.apache.spark.sparkextend

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.math._
import scala.util.matching.Regex
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
  val gcPeriod = 5

  val MapPowMatcher = """.+_SRDDmapPow(\d.\d)$""".r

  val GC = new Thread(new Runnable {
    def run () {
      while (true) { Thread.sleep(gcPeriod * 1000); manager.gc; }
    }
  })
  GC.start
  println("[SRDDMaster] GC has been set with period " + gcPeriod + " seconds.")
 
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

    case Delete(name) =>
      manager.deleteSRDD(name)
      println("[SRDDMaster] delete SRDD " + name)
      sender ! EXIT_SUCCESS

    case Count(name) =>
      val srdd = manager.getSRDD(name).get
      println("[SRDDMaster] count done.")
      sender ! srdd.rdd.count

    case Collect(name) =>
      var srdd = manager.getSRDD(name).get
      println("[SRDDMaster] collect done.")
      sender ! srdd.rdd.collect

    case Cache(name) =>
      val srdd = manager.getSRDD(name).get
      srdd.cache
      println("[SRDDMaster] cache done.")
      sender ! EXIT_SUCCESS

    case MapPow(name, p) =>
      val srdd = manager.getSRDD(name).get.asInstanceOf[SRDD[String]]
      var realName = name
      var realP = p

      if (!manager.hasSRDD(name + "_SRDDmapPow" + p)) {
        val r = MapPowMatcher.findFirstIn(name)
        if (r.isDefined) {
          val last = name.slice(name.lastIndexWhere(ch => (ch == 'w')) + 1, name.length)
          realName = name.replace("_SRDDmapPow" + last, "")
          realP = p + last.toDouble
          SRDDWrapper.wrap(realName + "_SRDDmapPow" + realP, manager, srdd.rdd.map(e => (pow(e.toDouble, p)).toString))
        }
        else
          SRDDWrapper.wrap(name + "_SRDDmapPow" + p, manager, srdd.rdd.map(e => (pow(e.toDouble, p)).toString))
      }
      else
        println("[SRDDMaster] Use existed SRDD " + name + "_SRDDmapPow" + p)
      println("[SRDDMaster] map.pow " + realName + "_SRDDmapPow" + realP + " done.")
      sender ! (realName + "_SRDDmapPow" + realP)

    case ReduceSum(name) =>
      val srdd = manager.getSRDD(name).get.asInstanceOf[SRDD[String]]
      val result = srdd.rdd.map(e => e.toDouble).reduce((a, b) => (a + b))
      println("[SRDDMaster] reduce.sum done.")
      sender ! result.toString

    case ReduceAvg(name) =>
      val rdd = manager.getSRDD(name).get.asInstanceOf[SRDD[String]].rdd
      val result = rdd.map(e => e.toDouble).reduce((a, b) => (a + b)) / rdd.count
      println("[SRDDMaster] reduce.avg done.")
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

