package com.darren.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
  * @Author Darren Zhang
  * @Date 2019-01-08
  * @Description TODO
  **/
object UDAF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("UDF")
      .getOrCreate()


    val sc = spark.sparkContext
    val names = Array("zhangsan", "lisi", "wangwu", "zhaoliu", "zhaoliu")
    val namesRDD = sc.parallelize(names, 4)
    val namesRowRDD = namesRDD.map(name => Row(name))
    val structType = StructType(Array(StructField("name", StringType, true)))
    val namesDF = spark.createDataFrame(namesRowRDD, structType)

    namesDF.createOrReplaceTempView("names")

    spark.udf.register("groupCount", new GroupCount)

    spark.sql("select name, groupCount(name) from names group by name")
      .show()

   // namesDF
      //.groupBy("name")
     // .selectExpr("name", "groupCount(name)")
     // .show()

  }
}
