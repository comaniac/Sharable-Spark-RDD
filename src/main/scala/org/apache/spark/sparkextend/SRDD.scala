package org.apache.spark.sparkextend

import org.apache.spark.sparkextend.ExitCode._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._

class SRDD_I(var name: String)
{
  def count(): Long = {
    val result: Option[Any] = SRDDClient.action(name, "count")
    result.get.asInstanceOf[Long]
  }

  def cache(): ExitCode = {
    SRDDClient.cache(name)
  }
}

class SRDD[T: ClassTag](uname: String, sc: SRDDManager, var rdd: RDD[T]) 
  extends RDD[T](sc.asInstanceOf[SparkContext], Nil) {

  val UniqueName = uname

  def this(name: String, sc: SRDDManager) = {
    this(name, sc, null)
  }

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
