package com.tens.sparkstreaming.resources

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  var socket: Socket = _
  var reader: BufferedReader = _
  //当接收器启动时回调这个函数
  override def onStart(): Unit = {
    runInThread{
      try{
        socket = new Socket(host, port)
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream,"utf-8"))
        var line: String = reader.readLine()
        while (socket.isConnected && line != null) {
          store(line)
          line = reader.readLine()
        }
      } catch {
        case e => println(e.getMessage)
      }finally {
        restart("重启receiver")
      }
    }
  }

  //当接收器停止时回调这个函数
  override def onStop(): Unit = {
    if(socket != null) {
      socket.close()
    }
    if(reader != null) {
      reader.close()
    }
  }

  def runInThread(f: => Unit)={
    new Thread() {
      override def run():Unit=f
    }.start()
  }
}
