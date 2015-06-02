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
import akka.event._
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor._
import akka.util._
import akka.pattern._
import akka.dispatch._
import com.typesafe.config._

class SRDDClient(
  var reval: ReturnValue,
  var masterUrl: String,
  var name: String, 
  var command: String, 
  var path: String, 
  var minPartitions: Int
  ) extends Actor with ActorLogging {

  var result = sendRequest()
  reval.set(result)

  def sendRequest(): Int = {
    val actor = context.actorSelection(masterUrl)

    implicit val timeout = Timeout(10 seconds)
    command match {
      case "textFile" =>
        Await.result(actor ? TextFile(name, path, minPartitions), timeout.duration).asInstanceOf[Int]
      case "objectFile" =>
        Await.result(actor ? ObjectFile(name, path, minPartitions), timeout.duration).asInstanceOf[Int]
      case _ =>
        println("[SRDDClient.ActorCreate] Invalied command: " + command)
        0
    }
  }

  def receive = {
    case _ =>
      println("[SRDDClient.ActorCreate] Receive unknown message.")
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

  def createSRDD(name: String, command: String, path: String, minPartitions: Int): Int = {
    val masterUrl = "akka.tcp://SRDDMaster@gemini.cs.ucla.edu:2552/user/SRDDMasterActor"
    val system = ActorSystem("ActorCreate", ConfigFactory.load("SRDD-akka-client"))
    var reval = new ReturnValue
    system.actorOf(Props(classOf[SRDDClient], reval, masterUrl, name, command, path, minPartitions), "createSRDD") 
    system.shutdown
    system.awaitTermination
    println("[SRDDClient] result " + reval.get)
    reval.get
  }

// coamniac: used for testing
  override def main(args: Array[String]) {
    val exitCode = createSRDD("testSRDD", "textFile", "/curr/cody/Course/cs249/Shareable-Spark-RDD/apps/test/testInput.txt", 2)
    if (exitCode == 1)
      println("[App] New SRDD")
    else
      println("[App] Use existed SRDD")
  }
}
