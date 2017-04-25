name := "server297"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.0.1",
    "org.apache.spark" % "spark-streaming_2.11" % "2.0.1",
    "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.1",
    "org.apache.hbase" % "hbase-common" % "1.2.3",
    "org.apache.hbase" % "hbase-server" % "1.2.3",
    "org.apache.htrace" % "htrace-core" % "3.2.0-incubating",
    "org.apache.hbase" % "hbase-client" % "1.2.3"
)