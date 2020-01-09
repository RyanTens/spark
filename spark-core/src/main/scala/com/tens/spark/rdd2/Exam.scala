package com.tens.spark.rdd2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*
    1.	数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割。
                  1516609143867 6 7 64 16
                  1516609143869 9 4 75 18
                  1516609143869 1 7 87 12
    2.	需求: 统计出每一个省份广告被点击次数的 TOP3
 */
object Exam {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Exam").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    //读取文件
    val lineRDD: RDD[String] = sc.textFile("src/main/resources/agent.log")
    val proAdAndOne: RDD[((String, String), Int)] = lineRDD.map(
      line => {
        val splits: Array[String] = line.split(" ")
        ((splits(1), splits(4)), 1)
      }
    )
    val proAdAndCount: RDD[((String, String), Int)] = proAdAndOne.reduceByKey(_ + _)
    val proAndAdCount: RDD[(String, (String, Int))] = proAdAndCount.map {
      case ((pro, ad), count) => (pro, (ad, count))
    }
    val proItAdAndCount: RDD[(String, Iterable[(String, Int)])] = proAndAdCount.groupByKey()
    val resultRDD: RDD[(String, List[(String, Int)])] = proItAdAndCount.mapValues(_.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
    resultRDD.collect.foreach(println)
    sc.stop()
  }
}
/*
RDD[(province,(ad,1))]
RDD[((province,ad),1)]
RDD[((province,ad1),count1),(province,ad2),count2),(province,ad3),count3)...]
RDD[(province,(ad1,count1)),(province,(ad2,count2)),(province,(ad3,count3))...]
RDD[(province,List((ad1,count1),(ad2,count2),(ad3,count3)))...]
 */