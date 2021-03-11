package com.reonomy.spark.test_session.job

import com.reonomy.spark.test_session.model.{InputModel, OutputModel}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ExampleJob extends GenericJob {
  def execute(spark: SparkSession): Unit = {
    import spark.implicits._

    spark
      .read
      .parquet("input-data")
      .as[InputModel]
      .map(im => {
        val fullName: String = Seq(im.firstName, im.lastName).mkString(" ")
        OutputModel(im.id, fullName)
      })
      .write
      .mode(SaveMode.Overwrite)
      .parquet("output-data")
  }
}
