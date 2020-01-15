package com.tens.sparkstreaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount2 {
  def createSSC() = {
    val conf: SparkConf = new SparkConf().setAppName("WordCount2").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck1")
    val params: Map[String, String] = Map[String, String](
      "group.id" -> "wordcount",
      "bootstrap.servers" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092"
    )
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      params,
      Set("Kwordcount2")
    ).flatMap{
      case (_,v) => v.split("\\W+").map((_,1))
    }.reduceByKey(_ + _).print()
    ssc
  }

  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck1", createSSC)
    ssc.start()
    ssc.awaitTermination()
  }

}
