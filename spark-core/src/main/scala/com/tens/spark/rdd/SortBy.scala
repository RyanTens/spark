package com.tens.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SortBy").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(3, 2, 1, 4, 5, 6,7,9, 10,1 ,11,12,7)
    val rdd: RDD[Int] = sc.makeRDD(list)
    val result: RDD[Int] = rdd.sortBy(x => x, false)
    result.collect().foreach(println)
    sc.stop()
  }
}
