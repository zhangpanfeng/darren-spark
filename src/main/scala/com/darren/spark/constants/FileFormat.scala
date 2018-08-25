package com.darren.spark.constants

object FileFormat extends Enumeration {

  val CSV_WITH_HEADER = Value(0, "csv")
  val CSV_WITHOUT_HEADER = Value(1, "csv")
  val PARQUET = Value(2, "parquet")
  val TEXT = Value(3, "text")
}
