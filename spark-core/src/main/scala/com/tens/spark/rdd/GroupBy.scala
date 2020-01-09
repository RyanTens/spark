package com.tens.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupBy").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(3, 2, 1, 4, 5, 6)
    val rdd: RDD[Int] = sc.makeRDD(list)
    val result: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)
    result.foreach(println)
    sc.stop()
  }
}
