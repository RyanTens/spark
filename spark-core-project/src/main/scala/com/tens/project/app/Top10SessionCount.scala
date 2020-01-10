package com.tens.project.app

import com.tens.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Top10SessionCount {
  def calcCategorySessionTop10(sc: SparkContext,categoryTop10: Array[CategoryCountInfo],userVisitActionRDD: RDD[UserVisitAction])={
    val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
    val filteredUserActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => {
      cids.contains(action.click_category_id)
    })
    val cidSidAndOne: RDD[((Long, String), Int)] = filteredUserActionRDD.map(action => {
      ((action.click_category_id, action.session_id), 1)
    })
    val cidAndSidCount: RDD[(Long, (String, Int))] = cidSidAndOne.reduceByKey(_ + _).map {
      case ((cid, sid), count) => (cid, (sid, count))
    }
    val groupedCidAndSidCount: RDD[(Long, Iterable[(String, Int)])] = cidAndSidCount.groupByKey()
    groupedCidAndSidCount.mapValues(it => {
      it.toList.sortBy(-_._2).take(10)
    })
  }

}
