package org.apache.spark.sparkextend

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._

class sRDD[T: ClassTag](name: String, sc: SparkContext, prev: RDD[T])
    extends RDD[T](prev) {

  // comaniac: Recorded information should be processed here.
  println("A new RDD \"" + name + "\" is created and is going to be recorded.")

  def showLocs = {
    val blockIds = this.partitions.indices.map(index => RDDBlockId(this.id, index)).toArray[BlockId]
    //val locs: Map[BlockId, Seq[BlockManagerId]] = BlockManager.blockIdsToBlockManagers(blockIds, sc.env, sc.env.blockManager.master)
    //println(blockIds.map { id => locs.getOrElse(id, Nil) })
  }

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

object sRDDWrapper {
  def wrap[T: ClassTag](name: String, sc: SparkContext, rdd : RDD[T]) : sRDD[T] = {
    new sRDD[T](name, sc, rdd)
  }
}
