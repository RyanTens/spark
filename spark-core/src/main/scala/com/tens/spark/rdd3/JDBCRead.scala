package com.tens.spark.rdd3

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JDBCRead {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("JDBCRead").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    //定义连接MySQL的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val userName = "root"
    val passWd = "root"

    val rdd: JdbcRDD[(Int, String)] = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      sql = "select id,name from test3 where ? <= id and id <= ?",
      lowerBound = 0,
      upperBound = 5,
      numPartitions = 2,
      (resultSet: ResultSet) => {
        (resultSet.getInt(1), resultSet.getString(2))
      }
    )
    rdd.collect.foreach(println)
    sc.stop()
  }
}
