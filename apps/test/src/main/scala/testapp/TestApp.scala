import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

// comaniac: Import extended package
import org.apache.spark.sparkextend._

object TestApp {
    def main(args : Array[String]) {
      val sc = get_spark_context("Test App")
      val rdd = sc.textFileSRDD("testSRDD", "/curr/cody/Course/cs249/Shareable-Spark-RDD/apps/test/testInput.txt")

    }

    def get_spark_context(appName : String) : SparkContextwithSRDD = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContextwithSRDD(conf)
    }
}

