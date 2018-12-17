package com.darren.test.resource

import java.util.ResourceBundle

object ResourceTest {

  def main(args: Array[String]): Unit = {
    val resource = ResourceBundle.getBundle("env.dev.environment")
    //val resource = ResourceBundle.getBundle("environment")

    println(resource.getString("name"))
//    for(key <- resource.keySet()){
//      println(key + " , " + resource.getString(key))
//    }
  }
}
