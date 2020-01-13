package com.tens.sparkstreaming.resources

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyReceiverDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MyReceiverDemo").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    val lineStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102", 9999))
    lineStream.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_ + _).print(100)

    ssc.start()
    ssc.awaitTermination()
  }
}
