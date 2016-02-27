name := "SparkWebLogNew"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.0",
  "org.apache.spark" % "spark-streaming_2.11" % "1.6.0",
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "1.6.0-M1",
  "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.0"
)