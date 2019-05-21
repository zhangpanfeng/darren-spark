package com.darren.spark.core

import org.apache.spark.sql.SparkSession

/**
  * @Author Darren Zhang
  * @Date 2019-01-12
  * @Description TODO
  **/
object TopN {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("TopN")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("test-in/top-n/topN.txt")

    // 第一步：转成“键值对”的形式
    val mapRdd = rdd.map(value => (value.toInt, value))


    // 第二步：根据“键”排序，然后取“值”返回
    val sortRdd = mapRdd.sortByKey(false).map(tuple => tuple._2)

    // 第三步：取出前N=3个

    sortRdd.take(3).foreach(println(_))

    spark.stop()
  }
}
