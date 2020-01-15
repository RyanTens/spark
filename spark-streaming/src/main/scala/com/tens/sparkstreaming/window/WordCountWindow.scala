package com.tens.sparkstreaming.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
  统计最近15秒内的单词的次数。每5秒统计一次
 */
object WordCountWindow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountWindow")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("./ck2")
    val sourceStream: DStream[String] = ssc.socketTextStream("hadoop102", 9999).window(Seconds(15), Seconds(5))
    sourceStream.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()

  }
}
