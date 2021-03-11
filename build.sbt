name := "SparkTestSession"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.6" % Test
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0" % Test
libraryDependencies += "org.mockito" %% "mockito-scala-scalatest" % "1.16.29" % Test
