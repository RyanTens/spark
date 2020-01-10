package com.tens.project.acc

import com.tens.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

//Map[(cid,"click"),1000]
//Map[(cid,"order"),1000]
//Map[(cid,"pay"),1000]
class CategoryAcc extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{
  //返回结果
  private var map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    val newAcc: CategoryAcc = new CategoryAcc
    map.synchronized{
      newAcc.map ++= map
    }
    newAcc
  }

  override def reset(): Unit = map.clear

  override def add(v: UserVisitAction): Unit = {
    //分别累加三种操作
    if(v.click_category_id != -1){  //表示点击事件
      map((v.click_category_id.toString,"click")) = map.getOrElseUpdate((v.click_category_id.toString,"click"),0) + 1
//      map += (v.click_category_id.toString,"click") -> (map.getOrElse((v.click_category_id.toString,"click"),0L) + 1L)
    }else if(v.order_category_ids != "null"){ //表示下单事件
      val cids: Array[String] = v.order_category_ids.split(",")
      cids.foreach(cid => {
//        map += (cid,"order") -> (map.getOrElse((cid,"order"),0L) + 1L)
        map((cid,"order")) = map.getOrElseUpdate((cid,"order"),0L) + 1L
      })
    }else if(v.pay_category_ids != "null"){ //表示支付事件
      val cids: Array[String] = v.pay_category_ids.split(",")
      cids.foreach(cid => {
        map += (cid,"pay") -> (map.getOrElse((cid,"pay"),0L) + 1L)
      })
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = other match {
      case o:CategoryAcc =>
        o.map.foldLeft(this.map){
          case (map,(cidAction,count)) =>
            map += (cidAction -> (this.map.getOrElse(cidAction,0L) + count))
        }
    }


  override def value: mutable.Map[(String, String), Long] = map
}
