package com.tens.project.app

import com.tens.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProjectApp {

  def main(args: Array[String]): Unit = {
    //1.初始化sc
    val conf: SparkConf = new SparkConf().setAppName("ProjectApp").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val lineRDD: RDD[String] = sc.textFile("E:\\user_visit_action.txt")
    //2.转换
    val userVisitActionRDD: RDD[UserVisitAction] = lineRDD.map(line => {
      val splits: Array[String] = line.split("_")
      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong
      )
    })
    val Top10 = CategoryTop10.statCategoryTop10(sc, userVisitActionRDD)
//    Top10.foreach(println)
    val top10Session: RDD[(Long, List[(String, Int)])] = Top10SessionCount.calcCategorySessionTop10(sc, Top10, userVisitActionRDD)
    top10Session.collect.foreach(println)
  }
}
