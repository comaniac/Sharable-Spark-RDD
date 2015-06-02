package org.apache.spark.sparkextend

import org.apache.spark.rdd._
import java.net._

// comaniac: Import extended package
import org.apache.spark.sparkextend._

// comaniac: Import Akka packages
import akka.actor._

trait SRDDControl extends Serializable

object SRDDControls {
  
  case class Test(name: String) extends SRDDControl

  case class ObjectFile(
    path: String, 
    minPartitions: Int
    ) extends SRDDControl

  case class TextFile(
    path: String,
    minPartitions: Int
    ) extends SRDDControl
}
