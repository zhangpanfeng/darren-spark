package com.darren.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author Darren Zhang
  * @Date 2019-03-19
  * @Description TODO
  **/
object SparkStreamTransform {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Stream Transform")

    val ssc = new StreamingContext(conf, Seconds(10))

    val blackList = Seq(("darren", true), ("david", false))
    val blackRdd = ssc.sparkContext.parallelize(blackList)
    blackRdd.foreach(println(_))


    val ds = ssc.socketTextStream("192.168.137.155", 8888)

    val pairDs = ds.map(item => {
      (item.split(" ")(0), item)
    })

    val clean = pairDs.transform(rdd => {
      rdd.leftOuterJoin(blackRdd).filter(touple => {
        touple._2._2.getOrElse(false) == false
      })
    })

    val result = clean.map(_._2._1)
    result.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
