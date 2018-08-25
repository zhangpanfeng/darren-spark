package com.darren.spark.util

import com.darren.spark.constants.FileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


object LoadUtil {

  /**
    * read file by format without header
    * @param spark
    * @param format
    * @param dataFiles
    * @return
    */
  def readFile(spark: SparkSession, format: FileFormat.Value, dataFiles: String*): DataFrame = {
    readFile(spark, format, null, dataFiles: _*)
  }

  /**
    * read file by format with header
    *
    * @param spark
    * @param format csv/parquet
    * @param schema
    * @param dataFiles
    * @return
    */
  def readFile(spark: SparkSession, format: FileFormat.Value, schema: StructType, dataFiles: String*): DataFrame = {

    val reader = format match {
      case FileFormat.CSV_WITH_HEADER => spark.read.option("header", "true").option("delimiter", "|").option("inferSchema", "true")
      case FileFormat.CSV_WITHOUT_HEADER => spark.read.option("header", "false").option("delimiter", "|").schema(schema)
      case _ => spark.read
    }

    reader
      .format(format.toString)
      .load(dataFiles: _*)
  }
}
