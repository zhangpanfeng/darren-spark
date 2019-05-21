package com.darren.spark.implicits

/**
  * @Author Darren Zhang
  * @Date 2019-01-09
  * @Description TODO
  **/

class Pen {
  def write(name: String): Unit = {
    println(name)
  }
}

object ImplicitContext {
  implicit val pen = new Pen
}

object ImplicitParameter {

  def signName(name: String)(implicit pen: Pen): Unit = {
    pen.write(name)
  }

  def signDate(name: String)(implicit pen: Pen): Unit = {
    pen.write(name)
  }

  def main(args: Array[String]): Unit = {
    import ImplicitContext._

    signName("zhangsan")
    signDate("2019-01-09")
  }
}
