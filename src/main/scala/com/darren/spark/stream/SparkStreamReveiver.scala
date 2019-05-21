package com.darren.spark.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author Darren Zhang
  * @Date 2019-03-18
  * @Description TODO
  **/
object SparkStreamReveiver {


  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Stream Receiver")

    //StreamingContext
    val ssc = new StreamingContext(conf, Seconds(10))


    //接收数据
    val ds = ssc.socketTextStream("192.168.137.155", 8888)

    //DStream是一个特殊的RDD
    val result = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()
    ssc.start()
    //等待结束
    ssc.awaitTermination()
  }

}
