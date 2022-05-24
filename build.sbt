name := "kafka-streams-ex"

version := "0.1"

scalaVersion := "2.13.8"

val scalaShortVersion = "2.13"
val kafkaVersion = "2.8.0"
libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "io.circe" %% "circe-core" % "0.14.1",
  "io.circe" %% "circe-generic" % "0.14.1",
  "io.circe" %% "circe-parser" % "0.14.1",
//  Test
  "org.scalatest" %% s"scalatest" % "3.1.1" % Test,
  "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test
)