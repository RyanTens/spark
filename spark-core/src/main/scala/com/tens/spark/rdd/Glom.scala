package com.tens.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Glom {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Glom").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(30, 20, 10, 40, 50, 60)
    val rdd: RDD[Int] = sc.makeRDD(list, 3)
    val result = rdd.glom()
    result.collect().foreach(x => println(x.mkString(", ")))
    sc.stop()
  }
}
