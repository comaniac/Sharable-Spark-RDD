package org.apache.spark.sparkextend

import org.apache.spark.sparkextend.ExitCode._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._

class SRDD_I[T: ClassTag](var name: String)
{
  def count(): Long = {
    val result: Option[Array[Any]] = SRDDClient.action(name, "count")
    (result.get)(0).asInstanceOf[Long]
  }

  def collect(): Array[T] = { // FIXME: Serialize
    val result: Option[Array[Any]] = SRDDClient.action(name, "collect")
    result.get.asInstanceOf[Array[T]]
  }

  def cache(): ExitCode = {
    SRDDClient.cache(name)
  }

  def map[T: ClassTag](command: String, args: Array[T]): SRDD_I[String] = {
    val stringArgs = args.map(e => e.toString)
    new SRDD_I[String](SRDDClient.map(name, command, stringArgs).get)
  }

  def reduce[T: ClassTag](command: String, args: Array[T] = null): String = {
    var stringArgs: Array[String] = null
    if (args != null)
      stringArgs = args.map(e => e.toString)
    SRDDClient.reduce(name, command, stringArgs).get
  }
}

class SRDD[T: ClassTag](uname: String, sc: SRDDManager, var rdd: RDD[T]) 
  extends RDD[T](sc.asInstanceOf[SparkContext], Nil) {

  val UniqueName = uname
  var FreqCnt = 0

  def this(name: String, sc: SRDDManager) = {
    this(name, sc, null)
  }

  def use = { 
    if (FreqCnt < 0)
      FreqCnt = 0
    else
      FreqCnt += 1 
  }
  def unuse = { FreqCnt -= 1 }
  def freq: Int = { FreqCnt }

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    val iter = new Iterator[T] {
      val nested = firstParent[T].iterator(split, context)

      def hasNext : Boolean = {
        nested.hasNext
      }

      def next : T = {
        nested.next
      }
    }
    iter
  }
}

object SRDDWrapper {

  def wrap[T: ClassTag](name: String, sc: SRDDManager, rdd: RDD[T]): ExitCode = { 
    if (!sc.hasSRDD(name)) {
      sc.bindSRDD(new SRDD[T](name, sc, rdd))
      CREATE_SUCCESS
    }
    else
      CREATE_IGNORE
  }
}
