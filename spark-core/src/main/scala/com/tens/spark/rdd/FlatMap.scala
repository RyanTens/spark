package com.tens.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FlatMap").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(30, 20, 10, 50, 60, 70)
    val rdd: RDD[Int] = sc.makeRDD(list)
    val result: RDD[Int] = rdd.flatMap(x => Array(x * x, x * x * x))
    println(result.collect().mkString(", "))
    sc.stop()
  }
}
