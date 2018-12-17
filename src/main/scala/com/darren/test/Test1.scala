package com.darren.test

class Test1() {
  println("sss")
}

object Test1{
  def main(args: Array[String]): Unit = {
    new Test1()

    import org.apache.spark.sql.functions._
    println(current_timestamp())
  }


  println("xxx")
  println("xxx")
}
