package com.tens.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Distinct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Distinct").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(31,2,2,3,3,1,1,4,4,5,6,10)
    val rdd: RDD[Int] = sc.makeRDD(list)
    val result: RDD[Int] = rdd.distinct()
    result.foreach(println)
    sc.stop()
  }
}
