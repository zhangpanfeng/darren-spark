package com.darren.test.practice.d20180820

import com.darren.spark.constants.FileFormat
import com.darren.spark.util.LoadUtil
import org.apache.spark.sql.functions.{col, isnull, sum, when}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Practices {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("WordCount")
      .getOrCreate()

    // set log level
    sparkSession.sparkContext.setLogLevel("WARN")

    val campaign = LoadUtil.readFile(sparkSession, FileFormat.CSV_WITH_HEADER, "C:\\scala\\data\\2018-01-25\\METADATA\\campaign.csv")
    //campaign.show()

    val grp28 = LoadUtil.readFile(sparkSession, FileFormat.CSV_WITH_HEADER, "C:\\scala\\data\\2018-01-28\\NORMAL\\FB\\*,5.csv")
    //grp28.show()

    val grp25 = LoadUtil.readFile(sparkSession, FileFormat.CSV_WITH_HEADER, "C:\\scala\\data\\2018-01-25\\NORMAL\\FB\\*,5.csv")
    //grp25.show()

    //    grp28.filter(col("DEMOGRAPHIC_ID") === 30000
    //      || col("DEMOGRAPHIC_ID") === 25000)
    //        .selectExpr(
    //          "CAMPAIGN_ID",
    //          "SITE_ID",
    //          "PLACEMENT_ID",
    //          "case when DEMOGRAPHIC_ID = 25000 then IMPRESSIONS else 0 end as 25000Imp",
    //          "case when DEMOGRAPHIC_ID = 30000 then IMPRESSIONS else 0 end as 30000Imp")
    //      .groupBy("CAMPAIGN_ID",
    //        "SITE_ID",
    //        "PLACEMENT_ID")
    //      .agg(
    //        sum("25000Imp") as "25000Imp" ,
    //        sum("30000Imp") as "30000Imp"
    //      ).as("grp28")
    //      .join(
    //        grp25.filter(col("DEMOGRAPHIC_ID") === 30000
    //          || col("DEMOGRAPHIC_ID") === 25000)
    //          .selectExpr(
    //            "CAMPAIGN_ID",
    //            "SITE_ID",
    //            "PLACEMENT_ID",
    //            "case when DEMOGRAPHIC_ID = 25000 then IMPRESSIONS else 0 end as 25000Imp",
    //            "case when DEMOGRAPHIC_ID = 30000 then IMPRESSIONS else 0 end as 30000Imp")
    //          .groupBy("CAMPAIGN_ID",
    //            "SITE_ID",
    //            "PLACEMENT_ID")
    //          .agg(
    //            sum("25000Imp") as "25000Imp" ,
    //            sum("30000Imp") as "30000Imp"
    //          ).as("grp25"),
    //
    //        List("CAMPAIGN_ID",
    //          "SITE_ID",
    //          "PLACEMENT_ID")
    //      ).where(col("grp28.30000Imp") < col("grp25.30000Imp") + 10000
    //    && (col("grp28.25000Imp") / col("grp28.25000Imp")) > 0.03)
    //      .join(campaign, List("CAMPAIGN_ID"), "left")
    //      .select("CAMPAIGN_ID",
    //        "CAMPAIGN_NAME",
    //        "PLACEMENT_ID",
    //      "grp25.30000Imp",
    //        "grp28.30000Imp",
    //        "grp28.25000Imp")
    //      .show()


    grp28
      .withColumn("IMPRESSIONS", col("IMPRESSIONS").cast(DoubleType))
      .groupBy("CAMPAIGN_ID",
        "SITE_ID",
        "PLACEMENT_ID").pivot("DEMOGRAPHIC_ID", List("25000", "30000")).sum("IMPRESSIONS")
      .select(col("CAMPAIGN_ID"),
        col("SITE_ID"),
        col("PLACEMENT_ID"),
        col("25000") as ("DEMO_25000"),
        col("30000") as ("DEMO_30000"))
      .selectExpr("CAMPAIGN_ID",
        "SITE_ID",
        "PLACEMENT_ID",
        "nvl(DEMO_25000, 0) as DEMO_25000",
        "nvl(DEMO_30000, 0) as DEMO_30000").as("grp28")
      .join(grp25
        .withColumn("IMPRESSIONS", col("IMPRESSIONS").cast(DoubleType))
        .groupBy("CAMPAIGN_ID",
          "SITE_ID",
          "PLACEMENT_ID").pivot("DEMOGRAPHIC_ID", List("25000", "30000")).sum("IMPRESSIONS")
        .select(col("CAMPAIGN_ID"),
          col("SITE_ID"),
          col("PLACEMENT_ID"),
          col("25000") as ("DEMO_25000"),
          col("30000") as ("DEMO_30000"))
        .selectExpr("CAMPAIGN_ID",
          "SITE_ID",
          "PLACEMENT_ID",
          "nvl(DEMO_25000, 0) as DEMO_25000",
          "nvl(DEMO_30000, 0) as DEMO_30000").as("grp25")
        ,
        List("CAMPAIGN_ID",
          "SITE_ID",
          "PLACEMENT_ID")
      )
      .where(col("grp28.DEMO_30000") < col("grp25.DEMO_30000") + 10000
        && (col("grp28.DEMO_25000") / col("grp28.DEMO_30000")) > 0.03)
      .join(campaign, List("CAMPAIGN_ID"), "left")
      .select("CAMPAIGN_ID",
        "CAMPAIGN_NAME",
        "PLACEMENT_ID",
        "grp25.DEMO_30000",
        "grp28.DEMO_30000",
        "grp28.DEMO_25000")
      .show()


    sparkSession.close()
  }
}