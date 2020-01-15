package com.tens.sparkstreaming.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountWindow2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCountWindow2").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("./ck3")
    val sourceStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    sourceStream.flatMap(_.split("\\W+"))
        .map((_,1))
        .reduceByKeyAndWindow(_ + _, _ - _, Seconds(15),filterFunc = _._2 > 0)
        .print(100)
    ssc.start()
    ssc.awaitTermination()
  }
}
