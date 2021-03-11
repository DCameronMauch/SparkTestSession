package com.reonomy.spark.test_session.job

import org.apache.spark.sql.SparkSession

trait GenericJob {
  def execute(spark: SparkSession): Unit
}
