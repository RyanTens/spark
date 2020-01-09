package com.tens.project.app

import com.tens.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 统计Top10热门品类
 * 统计每个品类点击的次数, 下单的次数和支付的次数
 */
object CategoryTop10 {
  def statCategoryCountTop10(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]) ={

  }
}
