package com.tens.spark.rdd2

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object ReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ReduceByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[(String, Int)] = List(("female", 1), ("male", 5), ("female", 5), ("male", 2))
    val rdd: RDD[(String, Int)] = sc.parallelize(list, 1)
    val rdd1: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
    rdd1.collect.foreach(println)
    sc.stop()
  }
}
