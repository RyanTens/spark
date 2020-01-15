package com.tens.realtime.app

import com.tens.realtime.bean.AdsInfo
import com.tens.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

trait App {
  def main(args: Array[String]): Unit = {
    //1.创建ssc
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealtimeApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //2.从kafka读数据
    val sourceStream: DStream[AdsInfo] = MyKafkaUtil.getKafkaStream(ssc, "ads_log").map(
      s => {
        val splits: Array[String] = s.split(",")
        AdsInfo(splits(0), splits(1), splits(2), splits(3), splits(4))
      }
    )

    //3.操作DStream
    doSomething(sourceStream)

    //4.启动ssc和阻止main方法退出
    ssc.start()
    ssc.awaitTermination()
  }

  def doSomething(ssc: DStream[AdsInfo])
}


