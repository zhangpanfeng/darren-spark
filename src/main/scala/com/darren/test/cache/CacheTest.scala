package com.darren.test.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Author Darren Zhang
  * @Date 2019-01-08
  * @Description TODO
  **/
object CacheTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("")
      .master("local")
      .getOrCreate()

    val text = cache(spark)
    for (i <- 1 to 3) {
      val start = System.currentTimeMillis();
      println(text.count())
      println("First Cost: " + (System.currentTimeMillis() - start))
    }
  }

  def cache(spark: SparkSession): RDD[String] = {
    val text = spark.sparkContext.textFile("test-in/load/txt/EventDB-DDL.sql")

    text.cache()

    val table = text.filter(_.startsWith("CREATE TABLE"))
    val unique = text.filter(_.startsWith("CREATE UNIQUE"))

    //text.unpersist()
    table.union(unique)
  }
}
