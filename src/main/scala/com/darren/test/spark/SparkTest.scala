package com.darren.test.spark

import org.apache.spark.sql.SparkSession

object SparkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val file = spark.sparkContext.textFile("test-in/test.txt")
    file.foreach(println)

    spark.close()
  }
}
