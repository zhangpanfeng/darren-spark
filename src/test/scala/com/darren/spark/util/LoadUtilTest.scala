package com.darren.spark.util

import com.darren.spark.TestWrapper
import com.darren.spark.constants.FileFormat
import com.darren.spark.util.ds.LoadTestDS
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class LoadUtilTest extends FunSuite with BeforeAndAfterAll with TestWrapper {

  override def beforeAll(): Unit = {

  }

  override def afterAll(): Unit = {
    spark.close()
  }

  test("Load") {
    val df = LoadUtil.readFile(spark, FileFormat.CSV_WITHOUT_HEADER, LoadTestDS.getSchema(), "test-in/test-load/")
    import spark.implicits._
    val ds = df.as[LoadTestDS]
    val firstRow = ds.first()
    ds.show()
    assert(firstRow.age == 2074)
    assert(firstRow.name == "2074_name")
  }

}
