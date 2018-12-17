package com.darren.spark

import org.scalatest.{BeforeAndAfterAll, FunSuite}

class Test extends FunSuite with BeforeAndAfterAll{

  override def beforeAll(): Unit ={
    println("before")
  }

  override def afterAll(): Unit = {
    println("after")
  }

  test("bb1"){
    println("1")
  }

  test("aa2"){
    println("2")
  }

  test("cc3"){
    println("3")
  }
}
