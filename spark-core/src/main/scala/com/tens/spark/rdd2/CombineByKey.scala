package com.tens.spark.rdd2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CombineByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    val rdd1: RDD[(String, Int)] = rdd.combineByKey(x => x, _ + _, _ + _)
    rdd1.collect.foreach(println)
    sc.stop()
  }
}
