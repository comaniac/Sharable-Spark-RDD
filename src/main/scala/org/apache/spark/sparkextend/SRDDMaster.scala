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

  val MapPowMatcher = """.+_pow(\d.\d)$""".r

  val GC = new Thread(new Runnable {
    def run () {
      while (true) { Thread.sleep(5000); manager.gc; }
    }
  })
  GC.start
 
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
      var realName = name
      var realP = p

      if (!manager.hasSRDD(name + "_pow" + p)) {
        val r = MapPowMatcher.findFirstIn(name)
        if (r.isDefined) {
          val last = name.slice(name.lastIndexWhere(ch => (ch == 'w')) + 1, name.length)
          realName = name.replace("_pow" + last, "")
          realP = p + last.toDouble
          SRDDWrapper.wrap(realName + "_pow" + realP, manager, srdd.rdd.map(e => (pow(e.toDouble, p)).toString))
        }
        else
          SRDDWrapper.wrap(name + "_pow" + p, manager, srdd.rdd.map(e => (pow(e.toDouble, p)).toString))
      }
      else
        println("[SRDDMaster] Use existed SRDD " + name + "_pow" + p)
      println("[SRDDMaster] map.pow " + realName + "_pow" + realP + " done.")
      sender ! (realName + "_pow" + realP)

    case ReduceSum(name) =>
      val srdd = manager.getSRDD(name).asInstanceOf[SRDD[String]]
      val result = srdd.rdd.map(e => e.toDouble).reduce((a, b) => (a + b))
      println("[SRDDMaster] reduce.sum done.")
      sender ! result.toString

    case ReduceAvg(name) =>
      val rdd = manager.getSRDD(name).asInstanceOf[SRDD[String]].rdd
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

