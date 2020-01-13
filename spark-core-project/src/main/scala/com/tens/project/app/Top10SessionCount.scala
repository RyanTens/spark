package com.tens.project.app

import com.tens.project.bean.{CategoryCountInfo, SessionInfo, UserVisitAction}
import com.tens.project.partitioner.CategoryPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

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

  def calcCategorySessionTop10_1(sc: SparkContext,categoryTop10: Array[CategoryCountInfo],userVisitActionRDD: RDD[UserVisitAction])={
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
    val resultRDD: RDD[(Long, List[SessionInfo])] = groupedCidAndSidCount.mapValues(it => {
      //创建一个Treeset来进行自动排序，每次只取前10
      var set: mutable.TreeSet[SessionInfo] = new mutable.TreeSet[SessionInfo]()
      it.foreach {
        case (sid, count) =>
          set += SessionInfo(sid, count)
          if (set.size > 10) set = set.take(10)
      }
      set.toList
    })
    resultRDD
  }

  def calcCategorySessionTop10_2(sc: SparkContext,categoryTop10: Array[CategoryCountInfo],userVisitActionRDD: RDD[UserVisitAction])={
    val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
    val filteredUserActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => {
      cids.contains(action.click_category_id)
    })
    val cidSidAndOne: RDD[((Long, String), Int)] = filteredUserActionRDD.map(action => {
      ((action.click_category_id, action.session_id), 1)
    })

    //聚合的时候让一个分区内只有一个cid，则后续不需要再次分组，提升性能
    val cidAndSidCount: RDD[(Long, (String, Int))] = cidSidAndOne.reduceByKey(new CategoryPartitioner(cids),_ + _).map {
      case ((cid, sid), count) => (cid, (sid, count))
    }
//    val groupedCidAndSidCount: RDD[(Long, Iterable[(String, Int)])] = cidAndSidCount.groupByKey()
    val resultRDD = cidAndSidCount.mapPartitions(it => {
      //创建一个Treeset来进行自动排序，每次只取前10
      var set: mutable.TreeSet[SessionInfo] = new mutable.TreeSet[SessionInfo]()
      var categoryId = 0L
      it.foreach {
        case (cid, (sid,count)) =>
          categoryId = cid
          set += SessionInfo(sid, count)
          if (set.size > 10) set = set.take(10)
      }
      set.map((categoryId, _)).toIterator
    })
    resultRDD
  }
}
