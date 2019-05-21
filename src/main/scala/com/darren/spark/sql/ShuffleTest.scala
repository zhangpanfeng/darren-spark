package com.darren.spark.sql

import com.darren.spark.constants.FileFormat
import com.darren.spark.model.ScaledUaCappingData
import com.darren.spark.util.LoadUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @Author Darren Zhang
  * @Date 2019-01-14
  * @Description TODO
  **/
object ShuffleTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("ShuffleTest")
      .getOrCreate()

    import spark.implicits._
    val ds = LoadUtil.readFile(spark, FileFormat.CSV_WITH_HEADER, "./test-in/load/etl_scaled_ua_capping_m2.csv").as[ScaledUaCappingData]

    ds.show()
    println("ds Partitions = " + ds.rdd.partitions.length)


    // 发生shuffle，partition增多
    val repartitionDs = ds.repartition(2)
    println("repartitionDs Partitions = " + repartitionDs.rdd.partitions.length)

    // 不发生shuffle，partition 和left保持一致
    val joinDs = ds.as("left")
      .join(repartitionDs.as("right"),
        Seq("level_1_id",
          "level_2_id",
          "level_3_id"
        ),
        "inner"
      )

    println("joinDs Partitions = " + joinDs.rdd.partitions.length)


    // 发生shuffle，partition增多, 变为spark sql的默认partition数量 200
    val groupDs = ds.groupBy("level_1_id")
      .agg(count("level_1_id"))

    println("groupDs Partitions = " + groupDs.rdd.partitions.length)

    // 不发生shuffle，partition 累加
    val unionDs = ds.union(ds)
    println("unionDs Partitions = " + unionDs.rdd.partitions.length)

    val distinctDs = ds.select("level_1_id",
      "level_2_id",
      "level_3_id")
      .distinct()

    // 发生shuffle，partition增多, 变为spark sql的默认partition数量 200
    println("distinctDs Partitions = " + distinctDs.rdd.partitions.length)
  }
}
