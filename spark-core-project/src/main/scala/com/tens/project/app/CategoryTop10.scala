package com.tens.project.app

import com.tens.project.acc.CategoryAcc
import com.tens.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * 统计Top10热门品类
 * 统计每个品类点击的次数, 下单的次数和支付的次数
 */
object CategoryTop10 {
  def statCategoryCountTop10(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]) ={
    val acc: CategoryAcc = new CategoryAcc
    sc.register(acc,"CategoryAcc")
    userVisitActionRDD.foreach(action =>{
      acc.add(action)
    })
    val cidAndMapCidActionCount: Map[String, mutable.Map[(String, String), Long]] = acc.value.groupBy(_._1._1)
    val categoryCountInfos: Array[CategoryCountInfo] = cidAndMapCidActionCount.map {
      case (cid, map) =>
        CategoryCountInfo(
          cid,
          map.getOrElse((cid, "click"), 0L),
          map.getOrElse((cid, "order"), 0L),
          map.getOrElse((cid, "pay"), 0L)
        )
    }.toArray
    categoryCountInfos.sortBy(info => (-info.clickCount,-info.orderCount,-info.payCount)).take(10)
  }
}
