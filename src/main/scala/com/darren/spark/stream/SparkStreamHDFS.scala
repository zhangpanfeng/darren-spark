package com.darren.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author Darren Zhang
  * @Date 2019-03-18
  * @Description TODO
  **/
object SparkStreamHDFS {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Stream HDFS")

    //StreamingContext
    val ssc = new StreamingContext(conf, Seconds(10))

    val ds = ssc.textFileStream("hdfs://10.227.6.89:8020/user/darren/test/")

    //DStream是一个特殊的RDD
    val result = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()
    ssc.start()
    //等待结束
    ssc.awaitTermination()
  }
}
