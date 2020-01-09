package com.tens.spark.accumulator

import org.apache.spark.util.AccumulatorV2

class MyMapAcc extends AccumulatorV2[Long,Map[String,Double]]{
  private var map: Map[String, Double] = Map[String, Double]()
  var count = 0L  //记录参加累加元素的个数
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[Long, Map[String, Double]] = {
    val newAcc: MyMapAcc = new MyMapAcc
    newAcc.map = map
    newAcc.count = count
    newAcc
  }

  override def reset(): Unit = {
    map = Map[String, Double]()
    count = 0
  }

  override def add(v: Long): Unit = {
    //sum,max,min
    count += 1
    map += "sum" -> (map.getOrElse("sum",0.0) + v)
    map += "max" -> map.getOrElse("max",Double.MinValue).max(v)
    map += "min" -> map.getOrElse("min",Double.MaxValue).min(v)
  }

  override def merge(other: AccumulatorV2[Long, Map[String, Double]]): Unit = {
    other match {
      case o: MyMapAcc =>
        this.count += o.count
        this.map += "sum" -> (this.map.getOrElse("sum",0.0) + o.map.getOrElse("sum",0.0))
        this.map += "max" -> this.map.getOrElse("max",Double.MinValue).max(o.map.getOrElse("max",Double.MinValue))
        this.map += "min" -> this.map.getOrElse("min",Double.MaxValue).min(o.map.getOrElse("min",Double.MaxValue))
      case _ => throw new IllegalArgumentException
    }
  }

  override def value: Map[String, Double] = {
    this.map += "avg" -> this.map.getOrElse("sum",0.0) / this.count
    this.map
  }
}
