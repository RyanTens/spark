package com.tens.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Map {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Map").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val arr: Array[Int] = Array(20, 30, 50, 10, 90, 80, 60, 40, 70)
    val rdd: RDD[Int] = sc.parallelize(arr)
    val resultRdd: RDD[Int] = rdd.map(_ * 2)
    println(resultRdd.collect().mkString(", "))

  }
}
