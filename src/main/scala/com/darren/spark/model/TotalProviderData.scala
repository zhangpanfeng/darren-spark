package com.darren.spark.model

import org.apache.spark.sql.types._

case class TotalProviderData(
                              level_1_id: String,
                              level_2_id: String,
                              level_3_id: String,
                              level_4_id: String,
                              level_5_id: String,
                              demographic_id: Int,
                              gender_alpha_code: String,
                              level_type_code: Int,
                              geography_code: Int,
                              platform_type_code: Int,
                              device_type_code: Int,
                              raw_unique_audience: Double,
                              raw_total_impressions: Double,
                              unknown_unique_audience: Double,
                              unknown_total_impressions: Double,
                              unique_audience_demo_distribution: Double,
                              total_impressions_demo_distribution: Double,
                              unknown_distributed_unique_audience: Double,
                              unknown_distributed_total_impressions: Double,
                              provider_unique_audience: Double,
                              provider_total_impressions: Double,
                              provider_total_duration: Double,
                              Intab_period_id: Long,
                              Intab_period_type_code: Int,
                              frequency: Double,
                              outlier_flag: Double,
                              adjusted_ua: Double,
                              adjusted_impression: Double,
                              main_sub_campaign: String,
                              total_frequency: Double
                            )

object TotalProviderData {
  def schema: StructType = {
    StructType(
      Array(
        StructField("level_1_id", StringType),
        StructField("level_2_id", StringType),
        StructField("level_3_id", StringType),
        StructField("level_4_id", StringType),
        StructField("level_5_id", StringType),
        StructField("demographic_id", IntegerType),
        StructField("gender_alpha_code", StringType),
        StructField("level_type_code", IntegerType),
        StructField("geography_code", IntegerType),
        StructField("platform_type_code", IntegerType),
        StructField("device_type_code", IntegerType),
        StructField("raw_unique_audience", DoubleType),
        StructField("raw_total_impressions", DoubleType),
        StructField("unknown_unique_audience", DoubleType),
        StructField("unknown_total_impressions", DoubleType),
        StructField("unique_audience_demo_distribution", DoubleType),
        StructField("total_impressions_demo_distribution", DoubleType),
        StructField("unknown_distributed_unique_audience", DoubleType),
        StructField("unknown_distributed_total_impressions", DoubleType),
        StructField("provider_unique_audience", DoubleType),
        StructField("provider_total_impressions", DoubleType),
        StructField("provider_total_duration", DoubleType),
        StructField("Intab_period_id", LongType),
        StructField("Intab_period_type_code", IntegerType),
        StructField("frequency", DoubleType),
        StructField("outlier_flag", DoubleType),
        StructField("adjusted_ua", DoubleType),
        StructField("adjusted_impression", DoubleType),
        StructField("main_sub_campaign", StringType),
        StructField("total_frequency", DoubleType)
      )
    )
  }
}
