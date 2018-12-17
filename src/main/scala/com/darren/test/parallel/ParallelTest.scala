package com.darren.test.parallel

import java.util.concurrent.CountDownLatch

import com.darren.spark.constants.FileFormat
import com.darren.spark.model.TotalProviderData
import com.darren.spark.util.{LoadUtil, WriteUtil}
import org.apache.spark.sql.SparkSession

object ParallelTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("ParallelTest")
      .getOrCreate()

    val latch = new CountDownLatch(2)

    import spark.implicits._
    val bigDs = LoadUtil.readFile(spark, FileFormat.CSV_WITH_HEADER, "test-in/load/etl_total_provider_data_m2_big.csv").as[TotalProviderData]
    val smallDs = LoadUtil.readFile(spark, FileFormat.CSV_WITH_HEADER, "test-in/load/etl_total_provider_data_m2_small.csv").as[TotalProviderData]

    // 第一个job
    WriteUtil.write(bigDs, "test-out/0/", FileFormat.CSV_WITH_HEADER, 1)

    // 第二个job
    WriteUtil.write(bigDs, "test-out/1/", FileFormat.CSV_WITH_HEADER, 1)


    new Thread(new Runnable {
      override def run(): Unit = {
        // 第三个job
        WriteUtil.write(bigDs, "test-out/2/", FileFormat.CSV_WITH_HEADER, 1)
        latch.countDown()
      }
    }).start()

    new Thread(new Runnable {
      override def run(): Unit = {
        // 第四个job
        WriteUtil.write(bigDs, "test-out/3/", FileFormat.CSV_WITH_HEADER, 1)
        latch.countDown()
      }
    }).start()

    Thread.sleep(1000)
    // 第五个job
    WriteUtil.write(smallDs, "test-out/4/", FileFormat.CSV_WITH_HEADER, 1)

    //Thread.sleep(1000)
    // 第六个job
    WriteUtil.write(smallDs, "test-out/5/", FileFormat.CSV_WITH_HEADER, 1)


    //Thread.sleep(60000)
    latch.await()
    spark.stop()
  }

}
