package com.tens.spark.rdd2

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitionBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("PartitionBy").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[String] = List("aa", "bb", "dd", "tens", "ryan")
    val rdd: RDD[String] = sc.parallelize(list,1)
    val rdd1: RDD[(String, Int)] = rdd.map((_, 1))
    //隐式转换：RDD=>pairRDD
    val rdd2: RDD[(String, Int)] = rdd1.partitionBy(new HashPartitioner(2))
    val rdd3: RDD[Array[(String, Int)]] = rdd2.glom()
    rdd3.collect.foreach(arr => println("p: " + arr.mkString(",")))
    sc.stop()
  }
}
