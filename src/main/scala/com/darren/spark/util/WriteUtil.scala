package com.darren.spark.util

import com.darren.spark.constants.FileFormat
import org.apache.spark.sql.{Dataset, SaveMode}

object WriteUtil {

  /**
    * Write dataSet to filesystem
    *
    * @param dataSet            Dataset
    * @param writePath          file path
    * @param format             file format
    * @param numberOfPartitions partition number
    * @param partitionColumns   partition columns
    * @tparam T any type
    */
  def write[T](dataSet: Dataset[T], writePath: String, format: FileFormat.Value, numberOfPartitions: Int, partitionColumns: String*): Unit = {
    val writer = format match {
      case FileFormat.CSV_WITH_HEADER => dataSet.coalesce(numberOfPartitions).write.option("header", "true").option("delimiter", "|")
      case FileFormat.CSV_WITHOUT_HEADER => dataSet.coalesce(numberOfPartitions).write.option("delimiter", "|")
      case _ => dataSet.coalesce(numberOfPartitions).write
    }

    writer
      .partitionBy(partitionColumns: _*)
      .mode(SaveMode.Overwrite)
      .format(format.toString)
      .save(writePath)
  }
}
