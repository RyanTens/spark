package com.tens.spark.rdd3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SerDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello tens", "tens", "ryan"), 2)
    val sercher: Search = new Search("hello")
    //调用了对象的方法
//    val result: RDD[String] = sercher.getMatchedRDD1(rdd)
    val result: RDD[String] = sercher.getMatchedRDD2(rdd)
    result.collect.foreach(println)
    sc.stop()
  }
}

//query为需要查询的子字符串
class Search(val query: String) {
  def isMatch(s:String)={
    s.contains(query)
  }
  def getMatchedRDD1(rdd: RDD[String])={
    rdd.filter(isMatch)
  }
  //过滤出包含query字符串
  def getMatchedRDD2(rdd: RDD[String])={
    val q = query
    rdd.filter(_.contains(q))
  }
}
