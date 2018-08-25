package com.darren.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

trait TestWrapper {
  lazy val spark : SparkSession = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("TestWrapper")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  lazy val hdfs : FileSystem = {
    FileSystem.get(new Configuration)
  }
}
