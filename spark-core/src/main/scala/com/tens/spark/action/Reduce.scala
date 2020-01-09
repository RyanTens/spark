package com.tens.spark.action

import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Reduce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Reduce").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[(String, Int)] = List(("a", 2), ("b", 1), ("c", 3), ("a", 4), ("b", 2), ("c", 1))
    val rdd: RDD[(String, Int)] = sc.parallelize(list,2)
    val result: (String, Int) = rdd.reduce((x,y) => (x._1 + y._1,x._2+y._2))
    val result2: collection.Map[String, Long] = rdd.countByKey()
    println(result)
    result2.foreach(println)
    sc.stop()
  }
}
