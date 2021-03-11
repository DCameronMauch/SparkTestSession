package com.reonomy.spark.test_session.job

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.{Map => MutableMap}

abstract class GenericJobTest extends AnyFunSuite with BeforeAndAfterAll with DatasetComparer with MockitoSugar {
  val persistence: MutableMap[String, DataFrame]

  print("Creating Spark Session")
  private val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("SparkTestSession")
    .master("local[*]")
    .getOrCreate()

  val sparkSessionSpy: SparkSession = spy(sparkSession)

  doAnswer[DataFrameReader] {
    println("level 1")
    val dataFrameReader: DataFrameReader = sparkSession.read
    val dataFrameReaderSpy: DataFrameReader = spy(dataFrameReader)

    doAnswer[String, DataFrame](path => {
      println("level 2")
      val key: String = Seq("parquet", path).mkString(":")
      val dataFrame: DataFrame = persistence.getOrElse(key, dataFrameReader.parquet(path))
      val dataFrameSpy: DataFrame = spy(dataFrame)

      doAnswer[DataFrameWriter[Row]] {
        println("level 3")
        val dataFrameWriter: DataFrameWriter[Row] = dataFrame.write
        val dataFrameWriterSpy: DataFrameWriter[Row] = spy(dataFrameWriter)

        doAnswer[String, Unit](path => {
          println("level 4")
          val key: String = Seq("parquet", path).mkString(":")
          persistence + (key -> dataFrame)
        }).when(dataFrameWriterSpy).parquet(any[String])

        dataFrameWriterSpy
      }.when(dataFrameSpy).write

      dataFrameSpy
    }).when(dataFrameReaderSpy).parquet(any[String])

    dataFrameReaderSpy
  }.when(sparkSessionSpy).read

  override def afterAll(): Unit = {
    println("Stopping Spark Session")
    sparkSession.stop()
    super.afterAll()
  }
}
