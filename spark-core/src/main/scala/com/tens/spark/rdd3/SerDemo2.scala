package com.tens.spark.rdd3

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SerDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("SerDemo2")
      .setMaster("local[2]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Search1]))
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello tens", "tens", "ryan"), 2)
    val searcher: Search1 = new Search1("hello")
    val result: RDD[String] = searcher.matchQuery(rdd)
    result.collect.foreach(println)
  }
}
case class Search1(query: String){
  def matchQuery(rdd: RDD[String])={
    rdd.filter(_.contains(query))
  }
}
