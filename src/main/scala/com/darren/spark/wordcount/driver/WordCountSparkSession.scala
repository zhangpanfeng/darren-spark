package com.darren.spark.wordcount.driver

import com.darren.spark.constants.FileFormat
import com.darren.spark.util.{HDFSUtil, LoadUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

/**
  * this version use SparkSession
  */
object WordCountSparkSession {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("ERROR: args.length != 2")
      return
    }

    val sparkSession = SparkSession.builder()
      .master("yarn")
      .appName("WordCount")
      .getOrCreate()

    // set log level
    sparkSession.sparkContext.setLogLevel("WARN")
    // input path
    val inputPath = args(0)
    println("inputPath = " + inputPath)

    //output path
    val outputPath = args(1)
    println("outputPath = " + outputPath)

    //delete output folder
    val hdfs = FileSystem.get(new Configuration)
    HDFSUtil.deleteFile(hdfs, outputPath)

    import sparkSession.implicits._
    val input = LoadUtil.readFile(sparkSession, FileFormat.TXT, inputPath.toString).as[String].rdd
    input.foreach(println(_))


    input.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(a => {
      val left = a._1;
      val right = a._2;
      left + ", " + right
    }).saveAsTextFile(outputPath.toString)

    sparkSession.close()
  }
}

