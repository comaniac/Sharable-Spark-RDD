package org.apache.spark.sparkextend

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.math._
import scala.reflect.{classTag, ClassTag}
import org.apache.spark.rdd._
import java.net._

// comaniac: Import extended package
import org.apache.spark.sparkextend._
import org.apache.spark.sparkextend.SRDDControls._
import org.apache.spark.sparkextend.ExitCode._

// comaniac: Import Akka packages
import akka.event._
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor._
import akka.util._
import akka.pattern._
import akka.dispatch._
import com.typesafe.config._

class SRDDClient extends Actor with ActorLogging {
  val masterUrl = "akka.tcp://SRDDMaster@gemini.cs.ucla.edu:2552/user/SRDDMasterActor"

  def getMasterUrl(): String = {
    masterUrl
  }

  def receive = {  
    case _ =>
      println("[SRDDClient] Receive a message.")
  }
}

class SRDDClientCreate(
  var reval: ReturnValue,
  var name: String, 
  var command: String, 
  var path: String, 
  var minPartitions: Int
  ) extends SRDDClient {

  val actor = context.actorSelection(masterUrl)

  implicit val timeout = Timeout(10 seconds)
  val code = command match {
    case "textFile" =>
      Await.result(actor ? TextFile(name, path, minPartitions), timeout.duration).asInstanceOf[ExitCode]
    case "objectFile" =>
      Await.result(actor ? ObjectFile(name, path, minPartitions), timeout.duration).asInstanceOf[ExitCode]
    case _ =>
      println("[SRDDClientCreate] Invalied command: " + command)
      CREATE_FAILURE
  }
  reval.setExitCode(code)

  override def receive = {
    case _ =>
      println("[SRDDClientCreate] Receive unknown message.")
  }
}

class SRDDClientAction(
  var reval: ReturnValue,
  var name: String, 
  var command: String
  ) extends SRDDClient {

  val actor = context.actorSelection(masterUrl)

  implicit val timeout = Timeout(10 seconds)
  val code = command match {
    case "count" =>
      val result = Await.result(actor ? Count(name), timeout.duration).asInstanceOf[Long]
      reval.setReturnCount(result)
      EXIT_SUCCESS
    case _ =>
      println("[SRDDClientAction] Invalied command: " + command)
      EXIT_FAILURE
  }
  reval.setExitCode(code)

  override def receive = {
    case _ =>
      println("[SRDDClientAction] Receive unknown message.")
  }
}

object SRDDClient extends App {
  def testSRDDMaster() = {
    val system = ActorSystem("SRDDClient", ConfigFactory.load("SRDD-akka-client"))
    println("[SRDDClient] test masterUrl")
    system.actorOf(Props(classOf[SRDDClient]), "test")
    Thread.sleep(1000)
    system.shutdown
  }

  def createSRDD(name: String, command: String, path: String, minPartitions: Int): ExitCode = {
    val system = ActorSystem("ActorCreate", ConfigFactory.load("SRDD-akka-client"))
    var reval = new ReturnValue
    system.actorOf(Props(classOf[SRDDClientCreate], reval, name, command, path, minPartitions), "createSRDD") 
    system.shutdown
    system.awaitTermination
    reval.getExitCode
  }

  def action(name: String, command: String): Option[Any] = {
    val system = ActorSystem("ActorCreate", ConfigFactory.load("SRDD-akka-client"))
    var reval = new ReturnValue
    system.actorOf(Props(classOf[SRDDClientAction], reval, name, command), "action") 
    system.shutdown
    system.awaitTermination

    if (reval.getExitCode == EXIT_SUCCESS)
      Some(reval.getReturnCount)
    else {
      println("[SRDDClient] Action failed due to unknown error.")
      Some(0.asInstanceOf[Long])
    }
  }

// coamniac: used for testing
  override def main(args: Array[String]) {
    val exitCode = createSRDD("testSRDD", "textFile", "/curr/cody/Course/cs249/Shareable-Spark-RDD/apps/test/testInput.txt", 2)
    if (exitCode == EXIT_SUCCESS)
      println("[App] New SRDD")
    else
      println("[App] Use existed SRDD")
  }
}
