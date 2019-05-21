package com.darren.spark.implicits

/**
  * @Author Darren Zhang
  * @Date 2019-01-09
  * @Description TODO
  **/

class SpecialPeople(val name: String)

class Older(val name: String)

class Child(val name: String)

class Adult(val name: String)

object ImplicitObject {

  implicit def object2SpecialPeople(obj: Object): SpecialPeople = {
    if (obj.getClass == classOf[Older]) {
      val older = obj.asInstanceOf[Older]
      new SpecialPeople(older.name)
    } else if (obj.getClass == classOf[Child]) {
      val child = obj.asInstanceOf[Child]
      new SpecialPeople(child.name)
    } else {
      throw new ClassCastException(obj.getClass + " can't be implicit convert to SpecialPeople")
    }
  }

  var sumTickets = 0

  def buySpecialTickets(specialPeople: SpecialPeople): Unit = {
    sumTickets += 1
    println(specialPeople.name + " buy " + sumTickets + " tickets")
  }

  def main(args: Array[String]): Unit = {
    val older = new Older("zzz")
    buySpecialTickets(older)

    val adult = new Adult("xxx")
    buySpecialTickets(adult)
  }
}
