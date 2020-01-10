package com.tens.project.app

import com.tens.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Top10SessionCount {
  def statCategoryTop10Session(sc: SparkContext,userVisitActionRDD: RDD[UserVisitAction],categoryCountInfos: Array[CategoryCountInfo]) ={
    //1.过滤出前10的品类的id的点击记录
    val cids: Array[Long] = categoryCountInfos.map(_.categoryId.toLong)
    val filterdUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
    //2.每个品类的top10session
    val cidSessionAndOne: RDD[((Long, String), Int)] = filterdUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
    val cidSessionAndCount: RDD[((Long, String), Int)] = cidSessionAndOne.reduceByKey(_ + _)
    val cidAndSidCount: RDD[(Long, (String, Int))] = cidSessionAndCount.map {
      case ((cid, sid), count) => (cid, (sid, count))
    }
    val groupedCidAndSidCount: RDD[(Long, Iterable[(String, Int)])] = cidAndSidCount.groupByKey()
    val resultRDD: RDD[(Long, List[(String, Int)])] = groupedCidAndSidCount.mapValues(it => {
      it.toList.sortBy(-_._2).take(10)
    })
    resultRDD
  }
}
