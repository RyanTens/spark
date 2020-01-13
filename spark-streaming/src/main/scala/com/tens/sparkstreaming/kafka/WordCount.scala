package com.tens.sparkstreaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    val params: Map[String, String] = Map[String, String](
      "group.id" -> "wordcount",
      "bootstrap.servers" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092")
    val sourceStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      params,
      Set("kwordcount"))
    sourceStream
        .map{
          case (_,v) => v
        }
        .flatMap(_.split("\\W+"))
        .map((_,1))
        .reduceByKey(_ + _)
        .print(100)
    ssc.start()
    ssc.awaitTermination()
  }
}
