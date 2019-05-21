package com.darren.test.stage

import com.darren.spark.constants.FileFormat
import com.darren.spark.model.ScaledUaCappingData
import com.darren.spark.util.LoadUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

/**
  * @Author Darren Zhang
  * @Date 2019-01-11
  * @Description TODO
  **/
object SelectStageTest {
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
      .select("level_1_id",
        "level_2_id",
        "level_3_id",
        "level_4_id",
        "demographic_id",
        "scaled_impressions")


    val a = ds.limit(10)
    val b = ds.select("level_1_id", "demographic_id").limit(10).distinct()
    val c = ds.select("demographic_id").limit(10).distinct()

    c.show()

    a.as("a")
        .join(b.as("b"), col("a.level_1_id") === col("b.level_1_id"))
      .join(c.as("c"), col("c.demographic_id") === col("b.demographic_id"))
      .select("a.level_1_id", "b.demographic_id")
        .show()

    c.as("c")
      .join(b.as("b"), col("c.demographic_id") === col("b.demographic_id"))
      .join(a.as("a"), col("a.level_1_id") === col("b.level_1_id"))
        .show()
    //ds
      //      .select("level_1_id",
      //        "level_2_id",
      //        "level_3_id",
      //        "level_4_id",
      //        "demographic_id",
      //        "scaled_impressions")
      //      .select("level_1_id",
      //        "level_2_id",
      //        "level_3_id",
      //        "level_4_id",
      //        "demographic_id")
     // .show()

    //ds.explain()

    Thread.sleep(60 * 1000 * 1000)
  }
}
