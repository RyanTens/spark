package com.tens.spark.rdd2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Union {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Union").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1: List[Int] = List(1, 2, 3, 4)
    val list2: List[Int] = List(5, 6, 7, 8, 9)
    val rdd1: RDD[Int] = sc.makeRDD(list1)
    val rdd2: RDD[Int] = sc.parallelize(list2)
    val result: RDD[Int] = rdd1.union(rdd2)
    result.foreach(println)
    sc.stop()
  }
}
