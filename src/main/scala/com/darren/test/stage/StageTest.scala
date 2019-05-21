package com.darren.test.stage

import com.darren.spark.constants.FileFormat
import com.darren.spark.model.ScaledUaCappingData
import com.darren.spark.util.LoadUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
  * @Author Darren Zhang
  * @Date 2019-01-08
  * @Description TODO
  **/
object StageTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val ds = LoadUtil.readFile(
      spark,
      FileFormat.CSV_WITH_HEADER,
      "./test-in/load/etl_scaled_ua_capping_m2.csv"
    ).as[ScaledUaCappingData]


    ds.groupBy("level_1_id",
      "level_2_id",
      "level_3_id",
      "level_4_id",
      "demographic_id"
    )
      .agg(sum("scaled_impressions").as("scaled_impressions"))
      .select("level_1_id",
        "level_2_id",
        "level_3_id",
        "level_4_id",
        "demographic_id",
        "scaled_impressions")
      .filter(col("level_1_id") === "xcm124083")
      //.withColumn("test", lit(null))
//      .union(
//      ds.groupBy("level_1_id",
//        "level_2_id",
//        "level_3_id",
//        "level_4_id"
//      )
//        .agg(sum("scaled_impressions").as("scaled_impressions"))
//        .selectExpr("level_1_id",
//          "level_2_id",
//          "level_3_id",
//          "level_4_id",
//          "30000 demographic_id",
//          "scaled_impressions")
//    )
      .show()

    Thread.sleep(60 * 1000 * 1000)
  }
}
