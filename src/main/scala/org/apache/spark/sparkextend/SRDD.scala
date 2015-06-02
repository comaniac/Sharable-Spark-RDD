package org.apache.spark.sparkextend

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._

class SRDD_I(name: String)
{
  ;
}

class SRDD[T: ClassTag](uname: String, sc: SRDDManager, prev: RDD[T]) 
  extends RDD[T](prev) {

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

   def wrap[T: ClassTag](name: String, sc: SRDDManager, rdd: RDD[T]): Int = { 
     if (!sc.hasSRDD(name)) {
       sc.bindSRDD(new SRDD[T](name, sc, rdd))
       1
     }
     else {
       0
     }
   }
 }
