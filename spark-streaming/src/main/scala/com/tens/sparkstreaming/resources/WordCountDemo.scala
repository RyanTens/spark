package com.tens.sparkstreaming.resources

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountDemo")
    //1.创建Spark Streaming的入口对象：StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //2.创建一个DStream，从数据源得到DStream
    val sourceStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //3.word count
    val wordAndCount: DStream[(String, Int)] = sourceStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //4.开始接收数据并计算
    wordAndCount.print(100)
    ssc.start()
    //5.阻止main线程结束，（要么手动退出，要么出现异常才退出主程序）
    ssc.awaitTermination()
  }
}
