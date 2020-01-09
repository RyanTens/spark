package com.tens.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Coalesce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Coalesce").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(31,2,2,3,3,1,1,4,4,5,6,10)
    val rdd: RDD[Int] = sc.makeRDD(list,4)
    println(rdd.partitions.length)
    val result: RDD[Int] = rdd.coalesce(2)
    println(result.partitions.length)
    val rdd2: RDD[Int] = result.coalesce(5, true)
    println(rdd2.partitions.length)

    sc.stop()
  }
}
