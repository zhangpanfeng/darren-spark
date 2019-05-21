package com.darren.test.cache

import org.apache.spark.sql.SparkSession

/**
  * @Author Darren Zhang
  * @Date 2019-01-09
  * @Description TODO
  **/
object CheckPointTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("./checkpoint")

    val text = spark.sparkContext.textFile("test-in/load/txt/EventDB-DDL.sql").filter(_.startsWith("CREATE TABLE"))
    text.checkpoint()

    text.first()

    spark.stop()
  }
}
