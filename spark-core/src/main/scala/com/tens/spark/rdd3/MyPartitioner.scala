package com.tens.spark.rdd3

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MyPartitioner {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MyPartitioner").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20, null, null)
    val rdd1: RDD[Any] = sc.parallelize(list1, 4)

    val kvRDD: RDD[(Any, Int)] = rdd1.map((_, 1))
    val result: RDD[(Any, Int)] = kvRDD.partitionBy(new MyPartitioner(2)).reduceByKey(new MyPartitioner(3), _ + _)
    val rdd: RDD[Array[(Any, Int)]] = result.glom()
    rdd.collect().foreach(a => {
      println("a: " + a.mkString(", "))
    })

    Thread.sleep(10000000)
    sc.stop()
  }
}

class MyPartitioner(val partitionerNum: Int) extends Partitioner {
  override def numPartitions: Int = partitionerNum

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ =>
      if (key.hashCode() % numPartitions < 0)
        key.hashCode() % numPartitions + numPartitions
      else key.hashCode() % numPartitions
  }

  override def hashCode(): Int = numPartitions

  override def equals(obj: Any): Boolean = obj match {
    case p: MyPartitioner => p.numPartitions == this.numPartitions
    case _ => false
  }
}
