package com.darren.spark.stream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author Darren Zhang
  * @Date 2019-03-22
  * @Description TODO
  **/
object SparkStreamKafkaDirectWindow {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Spark Stream Window")
    val ssc = new StreamingContext(conf, Seconds(5))
    val param = Map("kafka.metadata.broker" -> "centos1:9092,centos2:9092,centos3:9092")
    val topics = Set("darren")

    val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      param,
      topics
    )

    val result = ds.flatMap(_._2.split(" ")).map((_, 1)).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(60), Seconds(10))


    result.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
