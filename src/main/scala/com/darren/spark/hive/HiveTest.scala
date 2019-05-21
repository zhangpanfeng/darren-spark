package com.darren.spark.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateHiveTableContext

/**
  * @Author Darren Zhang
  * @Date 2019-01-11
  * @Description TODO
  **/
object HiveTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("hiveTest")
      .master("local")
      .getOrCreate()

//    spark.sql()
  }
}
