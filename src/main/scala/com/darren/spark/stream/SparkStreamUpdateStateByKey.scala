package com.darren.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author Darren Zhang
  * @Date 2019-03-19
  * @Description TODO
  **/
object SparkStreamUpdateStateByKey {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Stream UpdateStateByKey")

    val scc = new StreamingContext(conf, Seconds(10))
    scc.checkpoint("checkpoint")

    val ds = scc.socketTextStream("192.168.137.155", 8888)

    val result = ds.flatMap(_.split(" ")).map(item => (item, 1)).updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var newValue = state.getOrElse(0)
      for (value <- values) {
        newValue = newValue + value
      }

      Option(newValue)
    })

    result.print()

    scc.start()

    scc.awaitTermination()
  }
}
