package com.darren.spark.wordcount.driver

import java.io.{FileNotFoundException, IOException}

import com.darren.spark.util.HDFSUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}


/**
  * this version use SparkContext
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("ERROR: args.length != 2")
      return
    }

    // val conf = new SparkConf().setAppName("wordcount").setMaster("yarn-cluster")
    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)

    // input path
    val inputPath = args(0)
    println("inputPath = " + inputPath)

    //output path
    val outputPath = args(1)
    println("outputPath = " + outputPath)

    //delete output folder
    val hdfs = FileSystem.get(new Configuration)
    HDFSUtil.deleteFile(hdfs, outputPath)


    //read file
    val input = sc.textFile(inputPath)
    input.foreach(println(_))

    //word count
    input.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(a => {
      val left = a._1;
      val right = a._2;
      left + ", " + right
    }).saveAsTextFile(outputPath)
  }
}