package com.darren.spark.core

import java.util
import java.util.Collections

import org.apache.spark.sql.SparkSession

/**
  * @Author Darren Zhang
  * @Date 2019-01-12
  * @Description TODO
  **/
object GroupTopN {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("TopN")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("test-in/group-top-n/groupTopN.txt")

    val mapRdd = rdd.map(value => (value.split(" ")(0), value.split(" ")(1)))

    mapRdd.foreach(println(_))

    val groupRdd = mapRdd.groupByKey()

    groupRdd.foreach(println(_))

    //import scala.collection.JavaConversions._

        val resultRdd = groupRdd.map(row => {
          var list:List[Int] = Nil
          for(item <- row._2){
            list = list :+ item.toInt
          }

          (row._1, list.sortWith((left, right) => left > right).take(3))
        })

        resultRdd.foreach(println(_))

    resultRdd.foreach(row => {
      for(item <- row._2){
        println(row._1, item)
      }
    })

    //var list = Nil:List[Int]
    //var list:List[Int] = List()
//    var list:List[Int] = Nil
//    list = 1 :: list
//    list = 2 :: list
//    //list.sorted.foreach(println(_))
//    list.sortWith((left, right) => left > right).foreach(println(_))
  }
}
