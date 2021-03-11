package com.reonomy.spark.test_session.job

import org.apache.spark.sql.DataFrame
import scala.collection.mutable.{Map => MutableMap}

class ExampleJobTest extends GenericJobTest {
  import sparkSessionSpy.implicits._

  val persistence: MutableMap[String, DataFrame] = MutableMap(
    "parquet:input-data" -> Seq((1, "John", "Doe"), (2, "Sally", "Fields")).toDF("id", "firstName", "lastName")
  )

  test("example test") {
    ExampleJob.execute(sparkSessionSpy)

    val result: Option[DataFrame] = persistence.get("parquet:output-data")
    val expected: DataFrame = Seq((1, "John Doe"), (2, "Sally Fields")).toDF("id", "fullName")

    assert(result.isDefined)
    assertSmallDatasetEquality(result.get, expected)
  }
}
