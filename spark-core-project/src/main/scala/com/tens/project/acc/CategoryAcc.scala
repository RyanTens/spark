package com.tens.project.acc

import com.tens.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

private class CategoryAcc extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{
  private val map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    val newAcc: CategoryAcc = new CategoryAcc
    map.synchronized{
      newAcc.map ++= map
    }
    newAcc
  }

  override def reset(): Unit = map.clear()

  override def add(v: UserVisitAction): Unit = {
    
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = ???

  override def value: mutable.Map[(String, String), Long] = ???
}
