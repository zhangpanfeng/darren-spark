package com.darren.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @Author Darren Zhang
  * @Date 2019-01-08
  * @Description TODO
  **/
object UDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("UDF")
      .getOrCreate()

    val sc = spark.sparkContext
    val names = Array("zhangsan", "lisi", "wangwu", "zhaoliu")
    val namesRDD = sc.parallelize(names, 4)
    val namesRowRDD = namesRDD.map(name=>Row(name))
    val structType = StructType(Array(StructField("name", StringType, true)))
    val namesDF = spark.createDataFrame(namesRowRDD, structType)

    namesDF.createOrReplaceTempView("names")

    //spark.udf.register("length", (str:String) => str.length)
    spark.udf.register("length", length)

    spark.sql("select name, length(name) from names")
      .show()

    namesDF.selectExpr("name", "length(name)").show()

  }

//  def length(str:String): Int ={
//    str.length
//  }

  def length = (str:String) => str.length
}
