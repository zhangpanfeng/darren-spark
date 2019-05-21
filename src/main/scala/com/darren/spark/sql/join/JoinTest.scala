package com.darren.spark.sql.join

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

/**
  * @Author Darren Zhang
  * @Date 2019-05-21
  * @Description TODO
  **/
object JoinTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Join Test")
      .getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 110)
    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
    import spark.implicits._
    //定义两个集合，转换成dataframe
    var df1: DataFrame = Seq(
      ("0", "a"),
      ("1", "b"),
      ("2", "c"),
      ("3", "a"),
      ("4", "b"),
      ("5", "c"),
      ("6", "a"),
      ("7", "b"),
      ("8", "c"),
      ("9", "a"),
      ("10", "b"),
      ("11", "c")
    ).toDF("id", "name")

    df1 = df1.repartition(col("id"))


    var df2: DataFrame = Seq(
      ("0", "d"),
      ("1", "e"),
      ("2", "f")
    ).toDF("aid", "aname")
    println(df2.rdd.partitions.length)
    df2 = df2.repartition(col("aid"))
    println(df2.rdd.partitions.length)
    val result = df1.join(df2, $"id" === $"aid")

    result.show()

    result.explain()

    //Thread.sleep(100000)
    spark.stop()
  }
}
