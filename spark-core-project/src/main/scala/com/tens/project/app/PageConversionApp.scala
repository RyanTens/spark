package com.tens.project.app

import java.text.DecimalFormat

import com.tens.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PageConversionApp {
  def calcPageConversion(sc: SparkContext,userVisitActionRDD: RDD[UserVisitAction],pages: String)={
    //1.得到目标页面
    val splits: Array[String] = pages.split(",")
    //得到1，2，3，4，5，6
    val prePages: Array[String] = splits.slice(0, splits.length - 1)
    //得到2，3，4，5，6，7
    val postPages: Array[String] = splits.slice(1, splits.length)
    //拉链操作将页面关联------(1->2),(2->3),...
    val pageFlow: Array[String] = prePages.zip(postPages).map {
      case (prePage, postPage) => s"$prePage->$postPage"
    }

    //2.计算目标的每页点击量（分母）
    val pageAndCount: collection.Map[Long, Long] = userVisitActionRDD
      .filter(action => prePages.contains(action.page_id.toString))
      .map(action => (action.page_id, 1))
      .countByKey()

    //3.目标跳转的数量（分子）
    val sessionIdGrouped: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(_.session_id)
    val totalPageFlows: collection.Map[String, Long] = sessionIdGrouped.flatMap {
      case (sid, actionIt) =>
        val actions: List[UserVisitAction] = actionIt.toList.sortBy(_.action_time)
        val preActions: List[UserVisitAction] = actions.slice(0, actions.length - 1)
        val postActions: List[UserVisitAction] = actions.slice(1, actions.length)
        preActions.zip(postActions).map {
          case (preAction, postAction) => s"${preAction.page_id}->${postAction.page_id}"
        }.filter(flow => pageFlow.contains(flow))
    }.map((_, 1)).countByKey()

    //4.计算跳转率
    totalPageFlows.map{
      case (pageFlow, count) =>
        val formatter: DecimalFormat = new DecimalFormat("0.00%")
        val rate: Double = count.toDouble / pageAndCount(pageFlow.split("->")(0).toLong)
        (pageFlow,formatter.format(rate))
    }
  }
}
