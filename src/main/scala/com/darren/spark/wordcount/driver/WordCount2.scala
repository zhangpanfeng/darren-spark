package com.darren.spark.wordcount.driver

import com.darren.spark.util.HDFSUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object WordCount2 {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("WordCount")
      .getOrCreate()

    // set log level
    sparkSession.sparkContext.setLogLevel("WARN")

    val inputPath = new Path("/user/ocrdev02/test/wordCount.txt");
    val input = sparkSession.read.textFile(inputPath.toString).rdd
    input.foreach(println(_))

    val hdfs : FileSystem = FileSystem.get(new Configuration)
    val outputPath = new Path("/user/ocrdev02/test/output");
    HDFSUtil.deleteFile(hdfs, outputPath.toString)

    input.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(a => {
      val left = a._1;
      val right = a._2;
      left + ", " + right
    }).saveAsTextFile(outputPath.toString)
  }
}