package com.tens.spark.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccDemo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setAppName("AccDemo3").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 10, 9, 8, 5, 6, 7))
    val acc: MyMapAcc = new MyMapAcc
    sc.register(acc,"MyMapAcc")

    rdd.foreach(acc.add(_))

    println(acc.value)
    sc.stop()
  }
}
