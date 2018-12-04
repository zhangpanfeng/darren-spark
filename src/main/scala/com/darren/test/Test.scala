package com.darren.test

import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val text = spark.sparkContext.textFile("test-in/load/txt/EventDB-DDL.sql")

    //text.persist()
    for(i <- 1 to 3){
      val start = System.currentTimeMillis();
      println(text.count())
      println("Cost: " + (System.currentTimeMillis() - start))
    }
    spark.close()
  }


}