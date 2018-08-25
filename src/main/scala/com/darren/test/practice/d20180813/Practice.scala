package com.darren.test.practice.d20180813

import com.darren.spark.constants.FileFormat
import com.darren.spark.util.LoadUtil
import com.darren.test.practice.ds.NormalGrp
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, sum, when}
object Practice {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Practice")
      .getOrCreate()

    // set log level
    sparkSession.sparkContext.setLogLevel("WARN")

    val df = LoadUtil.readFile(sparkSession, FileFormat.CSV_WITH_HEADER, "C:\\scala\\data\\2018-01-25\\NORMAL\\FB\\2072,6.csv")

    import sparkSession.implicits._
    val base = df.filter(col("DEMOGRAPHIC_ID") === "30000")
      .select(
        "CAMPAIGN_ID",
        "SITE_ID",
        "PLACEMENT_ID",
        "DESIGNATED_MARKET_AREA_ID",
        "DEMOGRAPHIC_ID",
        "AGGREGATION_TYPE",
        "IMPRESSIONS"
      )
      //.withColumn("AGGREGATION_TYPE", col("AGGREGATION_TYPE").cast(IntegerType))
      .as[NormalGrp]

//    var estimatedDF = base
//    for (column <- base.columns) {
//      if (column == "AGGREGATION_TYPE") {
//        estimatedDF = estimatedDF.withColumn(column, col(column).cast(IntegerType))
//      }
//    }

//    val ds = estimatedDF.as[NormalGrp]
//
//    println(base.count())
//    val c = (1 to 5).foldLeft(base)((data, aggregationType) => data.union(base.map(aggregate(aggregationType)))).count()
//    println(c)
//
    (1 to 5).foldLeft(base)((data, aggregationType) => data.union(base.map(aggregate(aggregationType))))
      .groupBy(col("CAMPAIGN_ID"), col("AGGREGATION_TYPE"))
      .agg(sum("IMPRESSIONS") as "IMPRESSIONS")
      .show()


    base.map(_.copy(AGGREGATION_TYPE = 1)).show()
    base.map(aggregate(1)).show()
    println(List(1, 2, 3, 4, 5).foldLeft("0")((a, b) => a + "_" + b))

    sparkSession.close()
  }

  def aggregate(aggregationType: Int)(row: NormalGrp) = {
    aggregationType match {
      case 1 => {
        println("sss")
        row.copy(AGGREGATION_TYPE = 1)
      }
      case 2 => {
        row.copy(AGGREGATION_TYPE = 2)
      }
      case 3 => {
        row.copy(AGGREGATION_TYPE = 3)
      }
      case 4 => {
        row.copy(AGGREGATION_TYPE = 4)
      }
      case 5 => {
        row.copy(AGGREGATION_TYPE = 5)
      }
      case _ => {
        row.copy(AGGREGATION_TYPE = 7)
      }
    }

  }
}
