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

class SRDDClient(var masterUrl: String, var command: String, var path: String, var minPartitions: Int) extends Actor {
  sendRequest()

  def this(masterUrl: String) = {
    this(masterUrl, "test", null, 0)
  }

  def sendRequest() {
    val actor = context.actorSelection(masterUrl)

    command match {
      case "test" =>
        actor ! Identify("test")
      case "textFile" =>
        actor ! TextFile(path, minPartitions)
      case "objectFile" =>
        actor ! ObjectFile(path, minPartitions)
      case _ =>
        println("[SRDDClient] Invalied command: " + command)
    }
  }

  def receive = {
    case ActorIdentity(masterUrl, Some(actor)) =>
      println("[SRDDClient] identify pass, send test request.")
      actor ! Test("test")

    // TODO: Successed, RDD, Failed, etc.
  }
}

object SRDDClient extends App {
  val masterUrl = "akka.tcp://SRDDMaster@gemini.cs.ucla.edu:2552/user/SRDDMasterActor"

  def testSRDDMaster() = {
    val masterUrl = "akka.tcp://SRDDMaster@127.0.0.1:2552/user/SRDDMasterActor"
    val system = ActorSystem("SRDDClient", ConfigFactory.load("SRDD-akka-client"))
    println("[SRDDClient] test masterUrl: " + masterUrl)
    system.actorOf(Props(classOf[SRDDClient], masterUrl), "test")
    Thread.sleep(1000)
    system.shutdown
  }

  def createSRDD(command: String, path: String, minPartitions: Int) = {
    val masterUrl = "akka.tcp://SRDDMaster@gemini.cs.ucla.edu:2552/user/SRDDMasterActor"
    val system = ActorSystem("SRDDClient", ConfigFactory.load("SRDD-akka-client"))
    println("[SRDDClient] createSRDD masterUrl: " + masterUrl)
    system.actorOf(Props(classOf[SRDDClient], masterUrl, command, path, minPartitions), "createSRDD")
  }

// coamniac: used for testing
//  override def main(args: Array[String]) {
//    createSRDD("textFile", "/curr/cody/Course/cs249/Shareable-Spark-RDD/apps/test/testInput.txt", 2)
//  }
}
