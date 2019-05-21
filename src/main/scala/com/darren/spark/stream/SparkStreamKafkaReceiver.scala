package com.darren.spark.stream

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author Darren Zhang
  * @Date 2019-03-19
  * @Description TODO
  **/
object SparkStreamKafkaReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Stream Kafka Receiver")
    val ssc = new StreamingContext(conf, Seconds(10))


    val zkServer = "centos1:2181,centos2:2181,centos3:2181"

    val map = Map("darren" -> 1)

    val ds = KafkaUtils.createStream(ssc, zkServer, "wordcount", map)
    val result = ds.flatMap(touple => touple._2.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
