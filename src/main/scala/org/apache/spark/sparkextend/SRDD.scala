package org.apache.spark.sparkextend

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._

class SRDD[T: ClassTag](name: String, sc: SparkContextwithSRDD, prev: RDD[T])
    extends RDD[T](prev) {

  val UniqueName = name

  // Default RDD operations.

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
  def wrap[T: ClassTag](name: String, sc: SparkContextwithSRDD, rdd : RDD[T]) : SRDD[T] = {
    var newSRDD: SRDD[T] = null

    if (!sc.hasSRDD(name)) {
      newSRDD = new SRDD[T](name, sc, rdd)
      sc.bindSRDD(newSRDD)
      println("A new SRDD \"" + name + "\" is created and is going to be recorded.")
    }
    else {
//      newSRDD = sc.getSRDD(name)
      println("Find existed SRDD.")
    }

    newSRDD
  }
}
