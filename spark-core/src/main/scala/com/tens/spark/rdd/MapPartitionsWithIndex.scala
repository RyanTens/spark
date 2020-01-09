package com.tens.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapPartitionsWithIndex").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(30, 20, 10, 50, 40, 60,100)
    val rdd: RDD[Int] = sc.makeRDD(list,3)
    val result: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, it) => {
      it.map(x => (index, x))
    })
    result.foreach(println)

  }
}
