package com.tens.sparksql.udf

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator


case class User(name: String, age: Int)

case class AgeAvg(ageSum: Int, ageCount: Int, var ageAvg: Double = 0.0)

class MyAvgStrongType extends Aggregator[User,AgeAvg,Double]{
  //对缓冲区做初始化
  override def zero: AgeAvg = ???

  //分区内的聚合
  override def reduce(b: AgeAvg, a: User): AgeAvg = ???

  //分区间的聚合
  override def merge(b1: AgeAvg, b2: AgeAvg): AgeAvg = ???

  //返回最终的聚合结果
  override def finish(reduction: AgeAvg): Double = ???

  //缓冲的编码器
  override def bufferEncoder: Encoder[AgeAvg] = ???

  //输出的编码器
  override def outputEncoder: Encoder[Double] = ???
}
