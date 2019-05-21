package com.darren.spark.stream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author Darren Zhang
  * @Date 2019-03-20
  * @Description TODO
  **/
object SparkStreamKafkaDirect {

  val conf = new SparkConf().setMaster("local[1]").setAppName("Spark Stream Kafka Direct")

  val ssc = new StreamingContext(conf, Seconds(10))

  val param = Map("metadata.broker.list" -> "centos1:9092,centos2:9092,centos3:9092")

  val topics = Set("darren")

  val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc,
    param,
    topics
  )

  val result = ds.flatMap(_._2.split(" ")).map((_, 1)).reduceByKey(_ + _)

  result.print()

  ssc.start()

  ssc.awaitTermination()
}
