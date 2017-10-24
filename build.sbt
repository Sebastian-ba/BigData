name := "BigDataProject1"
scalaVersion := "2.11.7"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.5" % "test"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided"
// Useful for date and time data as Spark 2.0.0 has an issue when importing these
libraryDependencies += "joda-time" % "joda-time" % "2.9.9"
