package com.tens.project.acc

import com.tens.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

class CategoryAcc extends AccumulatorV2[UserVisitAction,Map[(String,String),Long]]{
  private val map: Map[(String, String), Long] = Map[(String, String), Long]()


  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = ???

  override def reset(): Unit = ???

  override def add(v: UserVisitAction): Unit = ???

  override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = ???

  override def value: Map[(String, String), Long] = ???
}
