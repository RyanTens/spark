package com.tens.spark.rdd3

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object JDBCWrite {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("JDBCWrite").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[(Int, String)] = List((3, "Andy"), (4, "Lina"), (5, "Jack"))
    val rdd: RDD[(Int, String)] = sc.parallelize(list)
    //定义连接MySQL的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val userName = "root"
    val passWd = "root"
    val sql = "insert into test3 values(?,?)"
    //一次写一条
    /*rdd.foreachPartition(it =>{
      Class.forName(driver)
      val conn: Connection = DriverManager.getConnection(url, userName, passWd)
      it.foreach{
        case (id,name) =>
          val ps: PreparedStatement = conn.prepareStatement(sql)
          ps.setInt(1,id)
          ps.setString(2,name)
          ps.execute()
          ps.close()
      }
      conn.close()
    })*/

    //一次写一批
    rdd.foreachPartition(it =>{
      Class.forName(driver)
      val conn: Connection = DriverManager.getConnection(url, userName, passWd)
      val ps: PreparedStatement = conn.prepareStatement(sql)
      it.foreach{
        case (id,name) => {
          ps.setInt(1,id)
          ps.setString(2,name)
          ps.addBatch()
        }
      }
      ps.executeBatch()
      conn.close()
    })
    rdd.collect.foreach(println)
    sc.stop()
  }
}
