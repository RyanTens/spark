package com.tens.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapPartitions").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(30, 20, 10, 50, 40, 60)
    val rdd: RDD[Int] = sc.makeRDD(list)
    val result: RDD[Int] = rdd.mapPartitions(it => it.map(x => x * x))
    result.foreach(println)

  }
}
