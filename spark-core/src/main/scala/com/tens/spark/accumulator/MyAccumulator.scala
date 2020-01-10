package com.tens.spark.accumulator

import org.apache.spark.util.AccumulatorV2

class MyAccumulator extends AccumulatorV2[Long,Long]{
  //缓存中间值
  private var sum = 0L

  //判断零值
  override def isZero: Boolean = sum == 0

  //复制累加器
  override def copy(): AccumulatorV2[Long, Long] = {
    val newAcc: MyAccumulator = new MyAccumulator
    newAcc.sum = sum  //当前缓存的sum赋值给新的acc
    newAcc
  }

  //重置累加器
  override def reset(): Unit = sum = 0

  //核心功能：累加
  override def add(v: Long): Unit = sum += v

  //合并
  override def merge(other: AccumulatorV2[Long, Long]): Unit = {
    other match {
      case o: MyAccumulator => this.sum += o.sum
      case _ => throw new IllegalArgumentException
    }
  }

  //返回最终累加后的值
  override def value: Long = this.sum
}
