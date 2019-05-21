package com.darren.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @Author Darren Zhang
  * @Date 2019-01-08
  * @Description TODO
  **/
class GroupCount extends UserDefinedAggregateFunction {
  //输入数据类型
  override def inputSchema: StructType = StructType(Array(StructField("str", StringType, true)))

  //中间数据类型
  override def bufferSchema: StructType = StructType(Array(StructField("count", IntegerType, true)))

  //返回数据类型
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  //初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  //局部累加
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  //全局累加
  override def merge(buffer: MutableAggregationBuffer, buffers: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + buffers.getAs[Int](0)
  }

  //最后一个方法可以更改你返回的数据样子
  //例如修改返回类型以及格式
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
}
