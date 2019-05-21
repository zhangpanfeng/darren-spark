package com.darren.spark.core

/**
  * @Author Darren Zhang
  * @Date 2019-01-12
  * @Description TODO
  **/
class SecondSortKey(val first: String, val second: Int) extends Ordered[SecondSortKey] with java.io.Serializable{
  override def <(that: SecondSortKey): Boolean = {
    if (first.length < that.first.length) {
      return true
    } else if (first.length == that.first.length && second < that.second) {
      return true
    } else {
      false
    }
  }

  override def >(that: SecondSortKey): Boolean = {
    if (first.length > that.first.length) {
      return true
    } else if (first.length == that.first.length && second > that.second) {
      return true
    } else {
      false
    }
  }

  override def <=(that: SecondSortKey): Boolean = {
    if (first.length <= that.first.length) {
      return true
    } else if (first.length == that.first.length && second <= that.second) {
      return true
    } else {
      false
    }
  }

  override def >=(that: SecondSortKey): Boolean = {
    if (first.length >= that.first.length) {
      return true
    } else if (first.length == that.first.length && second >= that.second) {
      return true
    } else {
      false
    }
  }

  override def compareTo(that: SecondSortKey): Int = {
    if (first.length - that.first.length != 0) {
      return first.length - that.first.length
    } else {
      return second - that.second
    }
  }

  override def compare(that: SecondSortKey): Int = {
    if (first.length - that.first.length != 0) {
      return first.length - that.first.length
    } else {
      return second - that.second
    }
  }


  override def toString = s"SecondSortKey($first, $second)"
}
