package com.darren.spark.util.ds

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class LoadTestDS(name: String,
                      age: Int)

object LoadTestDS {
  def getSchema(): StructType = {
    StructType(
      Array(
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    )
  }
}