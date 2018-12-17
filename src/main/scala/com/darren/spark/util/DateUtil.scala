package com.darren.spark.util

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DateUtil {
  val TIMESTAMP = "yyyy-MM-dd HH:mm:ss"

  /**
    * Obtain current date, format by default pattern "yyyy-MM-dd HH:mm:ss"
    *
    * @return
    */
  def now(): String = {
    now(TIMESTAMP)
  }

  /**
    * Obtain current date, format by pattern
    *
    * @param pattern
    * @return String
    */
  def now(pattern: String): String = {
    val formater = DateTimeFormatter.ofPattern(pattern)
    formater.format(LocalDateTime.now())
  }

}
