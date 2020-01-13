package com.tens.sparkstreaming.resources

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStramingWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreamingWordCount")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    val sc: SparkContext = ssc.sparkContext
    val queue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
    val rddDS: InputDStream[Int] = ssc.queueStream(queue, false)
    rddDS.reduce(_ + _).print
    ssc.start()

    while(true){
      val rdd: RDD[Int] = sc.parallelize(1 to 100)
      queue.enqueue(rdd)
      Thread.sleep(2000)
      println(queue.length)
    }
    ssc.awaitTermination()

  }
}
