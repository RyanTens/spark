package com.tens.spark.accumulator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BrodcastDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BrodcastDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(Array(2, 3, 5, 10, 9, 20, 3, 5, 7, 1, 63), 4)
    val set: Set[Int] = Set(3, 5)
    val bc: Broadcast[Set[Int]] = sc.broadcast(set)
    val fRdd: RDD[Int] = rdd.filter(bc.value.contains(_))
    fRdd.collect.foreach(println)
    sc.stop()
  }
}
