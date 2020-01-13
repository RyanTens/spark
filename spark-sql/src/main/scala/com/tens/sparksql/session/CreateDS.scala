package com.tens.sparksql.session

import org.apache.spark.sql.{Dataset, SparkSession}

object CreateDS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("CreateDS")
      .getOrCreate()
    import spark.implicits._
    val seq: Seq[Person] = Seq(Person("ryan", 18), Person("tens", 20))
    val ds: Dataset[Person] = seq.toDS()

  }
}
