package com.reonomy.spark.test_session

import com.reonomy.spark.test_session.job.ExampleJob
import org.apache.spark.sql.SparkSession

object Application {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkTestSession")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit =
    ExampleJob.execute(spark)
}