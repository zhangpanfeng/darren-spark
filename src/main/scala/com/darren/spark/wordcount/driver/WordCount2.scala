package com.darren.spark.wordcount.driver

import com.darren.spark.constants.FileFormat
import com.darren.spark.util.{HDFSUtil, LoadUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object WordCount2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\project\\program\\hadoop-common-2.7.1-bin-master")
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("WordCount")
      .getOrCreate()

    // set log level
    sparkSession.sparkContext.setLogLevel("WARN")


    val inputPath = "test-in/wordcount/"
    val outputPath = "test-out/wordcount/output"

    import sparkSession.implicits._
    val input = LoadUtil.readFile(sparkSession, FileFormat.TEXT, inputPath.toString).as[String].rdd
    input.foreach(println(_))

    val hdfs: FileSystem = FileSystem.get(new Configuration)

    HDFSUtil.deleteFile(hdfs, outputPath.toString)

    val result = input.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(a => {
      val left = a._1;
      val right = a._2;
      left + ", " + right
    })

    result.foreach(println)

    result.saveAsTextFile(outputPath.toString)

    sparkSession.close()
  }
}