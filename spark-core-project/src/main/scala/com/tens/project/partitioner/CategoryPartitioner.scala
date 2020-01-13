package com.tens.project.partitioner

import org.apache.spark.Partitioner

class CategoryPartitioner(cids: Array[Long]) extends Partitioner{
  private val map: Map[Long, Int] = cids.zipWithIndex.toMap

  //分区的个数设置和品类的id数保持一致，将来保证一个分区内只有一个品类的数据
  override def numPartitions: Int = cids.length

  override def getPartition(key: Any): Int = {
    key match {
      case (k: Long, _) => map(k)
    }
  }
}
