package com.tens.sparksql.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

import scala.collection.mutable

class MySum extends UserDefinedAggregateFunction{

  //数据的数据类型
  override def inputSchema: StructType = StructType(StructField("c", DoubleType)::Nil)

  //缓冲区的类型
  override def bufferSchema: StructType = StructType(StructField("sum", DoubleType)::Nil)

  //最终聚合后的数据类型
  override def dataType: DataType = DoubleType

  //相同的输入是否应该有相同的输出
  override def deterministic: Boolean = true

  //对缓冲区做初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
  }

  //分区内的聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){ //对传入数据做非空判断
      //得到传给聚合函数的值
    val value: Double = input.getDouble(0)
      //更新缓冲区
    buffer(0) = buffer.getDouble(0) + value
    }
  }

  //分区间的聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val value: Double = buffer2.getDouble(0)
    buffer1(0) = buffer1.getDouble(0) + value
  }

  //返回最后的聚合值
  override def evaluate(buffer: Row): Any = buffer(0)
}
