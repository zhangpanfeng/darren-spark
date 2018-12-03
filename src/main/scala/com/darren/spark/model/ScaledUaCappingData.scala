package com.darren.spark.model

import org.apache.spark.sql.types._

case class ScaledUaCappingData(
                                level_1_id: String,
                                level_2_id: String,
                                level_3_id: String,
                                level_4_id: String,
                                level_5_id: String,
                                demographic_id: Int,
                                level_type_code: Int,
                                geography_code: Int,
                                platform_type_code: Int,
                                device_type_code: Int,
                                intab_period_id: Long,
                                intab_period_type_code: Int,
                                scaled_unique_audience: Double,
                                scaled_impressions: Double
                              )

object ScaledUaCappingData {
  def schema: StructType = {
    StructType(
      Array(
        StructField("level_1_id", StringType),
        StructField("level_2_id", StringType),
        StructField("level_3_id", StringType),
        StructField("level_4_id", StringType),
        StructField("level_5_id", StringType),
        StructField("demographic_id", IntegerType),
        StructField("level_type_code", IntegerType),
        StructField("geography_code", IntegerType),
        StructField("platform_type_code", IntegerType),
        StructField("device_type_code", IntegerType),
        StructField("intab_period_id", LongType),
        StructField("intab_period_type_code", IntegerType),
        StructField("scaled_unique_audience", DoubleType),
        StructField("scaled_impressions", DoubleType)
      )
    )
  }
}