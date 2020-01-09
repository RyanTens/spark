package com.tens.spark.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AccDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))
    val acc: LongAccumulator = sc.longAccumulator

    val rdd2: RDD[(Int, Int)] = rdd.map(x => {
      acc.add(1)
      (x, 1)
    })
    rdd2.collect
    println(acc.value)
    sc.stop()
  }
}
