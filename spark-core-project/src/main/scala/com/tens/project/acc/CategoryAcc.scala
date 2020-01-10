package com.tens.project.acc

import com.tens.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

//Map[(cid,"click"),1000]
//Map[(cid,"order"),1000]
//Map[(cid,"pay"),1000]
class CategoryAcc extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{
  //统计的map
  private val map: mutable.Map[(String, String), Long] with Object = mutable.Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    val newAcc: CategoryAcc = new CategoryAcc
    map.synchronized{
      newAcc.map ++= map
    }
    newAcc
  }

  override def reset(): Unit = map.clear()

  //分别累加三种操作
  override def add(v: UserVisitAction): Unit = {
    if(v.click_category_id != -1){
      map((v.click_category_id.toString,"click")) = map.getOrElseUpdate((v.click_category_id.toString,"click"),0L) + 1
    }else if(v.order_category_ids != "null"){
      val cids: Array[String] = v.order_category_ids.split(",")
      cids.foreach(cid => {
        map((cid,"order")) = map.getOrElseUpdate((cid,"order"),0L) + 1
      })
    }else if(v.pay_category_ids != "null"){
      val cids: Array[String] = v.pay_category_ids.split(",")
      cids.foreach(cid => {
        map((cid,"pay")) = map.getOrElseUpdate((cid,"pay"),0L) + 1
      })
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = other match {
    case o: CategoryAcc =>
      o.map.foreach{
        kv => map.put(kv._1,map.getOrElse(kv._1,0L) + kv._2)
      }
    case _ => throw new UnsupportedOperationException
  }

  override def value: mutable.Map[(String, String), Long] = map
}
