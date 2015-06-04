package org.apache.spark.sparkextend

import org.apache.spark.rdd._
import java.net._

// comaniac: Import extended package
import org.apache.spark.sparkextend._

// comaniac: Import Akka packages
import akka.actor._

object ExitCode extends Enumeration {
  type ExitCode = Value
  val EXIT_SUCCESS = Value
  val EXIT_FAILURE = Value
  val CREATE_SUCCESS = Value
  val CREATE_FAILURE = Value
  val CREATE_IGNORE = Value
}

trait SRDDControl extends Serializable
object SRDDControls {
  
  case class Test(name: String) extends SRDDControl

  case class ObjectFile(
    name: String, 
    path: String, 
    minPartitions: Int
    ) extends SRDDControl

  case class TextFile(
    name: String,
    path: String,
    minPartitions: Int
    ) extends SRDDControl

  case class Count(
    name: String
    ) extends SRDDControl

  case class Collect(
    name: String
  ) extends SRDDControl

  case class MapPow(
    name: String,
    power: Int
  ) extends SRDDControl

  case class ReduceSum(
    name: String
  ) extends SRDDControl

  case class Cache(
    name: String
    ) extends SRDDControl
}

import ExitCode._
class ReturnValue {
  var exitCode: ExitCode = EXIT_SUCCESS
  var returnCount: Long = -1
  var returnCollect: Array[Any] = null
  var returnSRDD: SRDD_I[String] = null
  var returnStr: String = null

  def setExitCode(v: ExitCode) { exitCode = v }
  def getExitCode(): ExitCode = { exitCode }

  def setReturnCount(v: Long) { returnCount = v }
  def getReturnCount(): Long = { returnCount }

  def setReturnCollect(a: Array[Any]) { returnCollect = a }
  def getReturnCollect(): Array[Any] = { returnCollect }

  def setReturnSRDD(srdd: SRDD_I[String]) { returnSRDD = srdd }
  def getReturnSRDD(): SRDD_I[String] = { returnSRDD }

  def setReturnString(s: String) { returnStr = s }
  def getReturnString(): String = { returnStr }
}

