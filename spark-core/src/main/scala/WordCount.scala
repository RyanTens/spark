import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf对象，并设置App名字
    val conf: SparkConf = new SparkConf().setAppName("WordCount")

    //2.创建SparkContext对象
    val sc: SparkContext = new SparkContext(conf)

    //3.使用sc创建RDD并执行相应的transformation和action
    sc.textFile("/input")
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_ + _)
      .saveAsTextFile("/result")

    //4.关闭连接
    sc.stop()
  }
}
