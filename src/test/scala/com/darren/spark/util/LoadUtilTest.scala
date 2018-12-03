package com.darren.spark.util

import com.darren.spark.TestWrapper
import com.darren.spark.constants.FileFormat
import com.darren.spark.model.{ScaledUaCappingData, TotalProviderData}
import com.darren.spark.util.ds.LoadTestDS
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class LoadUtilTest extends FunSuite with BeforeAndAfterAll with TestWrapper {
//  val spark = SparkSession.builder()
//    .master("local")
//    .appName("TestWrapper")
//    .getOrCreate()
  override def beforeAll(): Unit = {
    // prepare something
  }

  override def afterAll(): Unit = {
    spark.close()
  }

  test("Test-Load-CSV") {
    val df = LoadUtil.readFile(spark, FileFormat.CSV_WITHOUT_HEADER, LoadTestDS.getSchema(), "test-in/load/csv/")
    import spark.implicits._
    val ds = df.as[LoadTestDS]
    val firstRow = ds.first()
    ds.show()
    assert(firstRow.age == 2074)
    assert(firstRow.name == "2074_name1")
  }

  test("Test-Load-TXT") {
    val df = LoadUtil.readFile(spark, FileFormat.TEXT, LoadTestDS.getSchema(), "test-in/load/txt/test1.txt")
    //val df = LoadUtil.readFile(spark, FileFormat.TEXT, "test-in/load/txt/test.txt")
    df.show()
    val firstRow = df.first().getString(0)
    println(firstRow)

    assert(firstRow == "hello|10")

  }


  test("test load csv") {
    val df = LoadUtil.readFile(this.spark, FileFormat.CSV_WITH_HEADER, "./datamart/test-in/etl_scaled_ua_capping_m2.csv")
    assert(df.count() > 0)
  }

  test("test load ScaledUaCappingData with header") {
    import spark.implicits._
    val ds = LoadUtil.readFile(this.spark, FileFormat.CSV_WITH_HEADER, "./datamart/test-in/etl_scaled_ua_capping_m2.csv").as[ScaledUaCappingData]
    ds.show(1)
    println(ds.count())
    assert(ds.count() > 0)
  }

  test("test load ScaledUaCappingData without header") {
    import spark.implicits._
    val ds = LoadUtil.readFile(this.spark, FileFormat.CSV_WITHOUT_HEADER, ScaledUaCappingData.schema, "./datamart/test-in/etl_scaled_ua_capping_m2_without_header.csv").as[ScaledUaCappingData]
    ds.show(1)
    println(ds.count())
    assert(ds.count() > 0)
  }

  test("test load TotalProviderData with header") {
    import spark.implicits._
    val ds = LoadUtil.readFile(this.spark, FileFormat.CSV_WITH_HEADER, "test-in/load/etl_total_provider_data_m2.csv").as[TotalProviderData]
    ds.show(1)
    println(ds.count())
    assert(ds.count() > 0)
  }

  test("test load TotalProviderData without header") {
    import spark.implicits._
    val ds = LoadUtil.readFile(this.spark, FileFormat.CSV_WITHOUT_HEADER, TotalProviderData.schema, "test-in/load/etl_total_provider_data_m2_without_header.csv").as[TotalProviderData]
    //val ds = LoadUtil.readFile(this.spark, FileFormat.CSV_WITHOUT_HEADER,"test-in/load/etl_total_provider_data_m2_without_header.csv").as[TotalProviderData]
    //val ds = LoadUtil.readFile(this.spark, FileFormat.CSV_WITHOUT_HEADER,"test-in/load/etl_total_provider_data_m2_without_header.csv")
    ds.show(1)
    println(ds.count())
    assert(ds.count() > 0)
  }

}
