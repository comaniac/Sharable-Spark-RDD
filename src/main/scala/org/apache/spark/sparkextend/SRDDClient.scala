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
    case "collect" => // FIXME
      val result = Await.result(actor ? Collect(name), timeout.duration).asInstanceOf[Array[Any]]
      println("[SRDDClientAction] Array length " + result.length)
      reval.setReturnCollect(result)
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

class SRDDClientMap(
  var reval: ReturnValue,
  var name: String, 
  var command: String,
  var args: Array[String]
  ) extends SRDDClient {

  val actor = context.actorSelection(masterUrl)

  implicit val timeout = Timeout(10 seconds)
  val code = command match {
    case "pow" =>
      val result = Await.result(actor ? MapPow(name, args(0).toInt), timeout.duration).asInstanceOf[String]
      reval.setReturnString(result)
      EXIT_SUCCESS

    case _ =>
      println("[SRDDClientMap] Invalied command: " + command)
      EXIT_FAILURE
  }
  reval.setExitCode(code)

  override def receive = {
    case _ =>
      println("[SRDDClientAction] Receive unknown message.")
  }
}

class SRDDClientReduce(
  var reval: ReturnValue,
  var name: String, 
  var command: String,
  var args: Array[String]
  ) extends SRDDClient {

  val actor = context.actorSelection(masterUrl)

  implicit val timeout = Timeout(10 seconds)
  val code = command match {
    case "sum" =>
      val result = Await.result(actor ? ReduceSum(name), timeout.duration).asInstanceOf[String]
      reval.setReturnString(result)
      EXIT_SUCCESS
    case "avg" =>
      val result = Await.result(actor ? ReduceAvg(name), timeout.duration).asInstanceOf[String]
      reval.setReturnString(result)
      EXIT_SUCCESS
    case _ =>
      println("[SRDDClientReduce] Invalied command: " + command)
      EXIT_FAILURE
  }
  reval.setExitCode(code)

  override def receive = {
    case _ =>
      println("[SRDDClientReduce] Receive unknown message.")
  }
}

class SRDDClientCache(
  var reval: ReturnValue,
  var name: String
  ) extends SRDDClient {

  val actor = context.actorSelection(masterUrl)

  implicit val timeout = Timeout(10 seconds)
  val code = Await.result(actor ? Cache(name), timeout.duration).asInstanceOf[ExitCode]
  reval.setExitCode(code)

  override def receive = {
    case _ =>
      println("[SRDDClientCache] Receive unknown message.")
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

  def action(name: String, command: String): Option[Array[Any]] = {
    val system = ActorSystem("ActorAction", ConfigFactory.load("SRDD-akka-client"))
    var reval = new ReturnValue
    system.actorOf(Props(classOf[SRDDClientAction], reval, name, command), "action") 
    system.shutdown
    system.awaitTermination

    if (reval.getExitCode == EXIT_SUCCESS)
      command match {
        case "count" =>
          val r = Array[Any](1)
          r(0) = reval.getReturnCount
          Some(r)
        case "collect" =>
          Some(reval.getReturnCollect)
      }
    else {
      println("[SRDDClient] Action failed due to unknown error.")
      None
    }
  }

   def map(name: String, command: String, args: Array[String]): Option[String] = {
    val system = ActorSystem("ActorMap", ConfigFactory.load("SRDD-akka-client"))
    var reval = new ReturnValue
    system.actorOf(Props(classOf[SRDDClientMap], reval, name, command, args), "map") 
    system.shutdown
    system.awaitTermination

    if (reval.getExitCode == EXIT_SUCCESS) {
      command match {
        case "pow" =>
          Some(reval.getReturnString)
      }
    }
    else {
      println("[SRDDClient] Map failed due to unknown error.")
      None
    }
  }

   def reduce(name: String, command: String, args: Array[String]): Option[String] = {
    val system = ActorSystem("ActorReduce", ConfigFactory.load("SRDD-akka-client"))
    var reval = new ReturnValue
    system.actorOf(Props(classOf[SRDDClientReduce], reval, name, command, args), "reduce")
    system.shutdown
    system.awaitTermination

    if (reval.getExitCode == EXIT_SUCCESS) {
      command match {
        case "sum" =>
          Some(reval.getReturnString)
        case "avg" =>
          Some(reval.getReturnString)
      }
    }
    else {
      println("[SRDDClient] Reduce failed due to unknown error.")
      None
    }
  }

  def cache(name: String): ExitCode = {
    val system = ActorSystem("ActorCreate", ConfigFactory.load("SRDD-akka-client"))
    var reval = new ReturnValue
    system.actorOf(Props(classOf[SRDDClientCache], reval, name), "cacheSRDD") 
    system.shutdown
    system.awaitTermination
    reval.getExitCode
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
