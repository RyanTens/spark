package com.tens.sparksql.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

class MyAvg extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("inputColumn",DoubleType)::Nil)

  //缓冲区中值的类型
  override def bufferSchema: StructType = {
    StructType(StructField("sum",DoubleType)::StructField("count",LongType)::Nil)
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 1L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(buffer2.isNullAt(0)){
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0) / buffer.getLong(1)
  }
}
