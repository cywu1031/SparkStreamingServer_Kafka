name := "server297"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.0.1",
    "org.apache.spark" % "spark-streaming_2.11" % "2.0.1",
    "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.1"
)