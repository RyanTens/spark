package com.tens.spark.rdd2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AggregateByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    //求分区内最大值之和以及最小值之和
    val result1: RDD[(String, (Int, Int))] = rdd.aggregateByKey((Int.MinValue, Int.MaxValue))(
      {
        case ((max, min), v) => (v.max(max), v.min(min))
      },
      {
        case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2)
      }
    )
    result1.collect().foreach(println)
    //求平均值
    val result2: RDD[(String, Double)] = rdd.aggregateByKey((0, 0))(
      {
        case ((sum, count), v) => (sum + v, count + 1)
      },
      {
        case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
      }
    ).map {
      case (word, (sum, count)) => (word, sum.toDouble / count)
    }
    result2.collect.foreach(println)
    sc.stop()
  }
}
