package com.darren.spark.core

import org.apache.spark.sql.SparkSession

/**
  * @Author Darren Zhang
  * @Date 2019-01-12
  * @Description TODO
  **/
object SecondSort {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("TopN")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("test-in/second-sort/secondSort.txt")

    val mapRdd = rdd.map(row => {
      val array = row.split(" ")
      (new SecondSortKey(array(0), array(1).toInt), row)
    })

    val sortRdd = mapRdd.sortByKey(false)

    sortRdd.foreach(println(_))

    sortRdd.map(_._2).foreach(println(_))
  }
}
