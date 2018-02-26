import Dependencies.{curator, _}
import Versions._
import sbt.ExclusionRule

name in ThisBuild := "exactly-once-streams"

organization in ThisBuild := "seglo"

scalaVersion in ThisBuild := Versions.Scala_2_12_Version

resolvers += "Confluent Maven" at "http://packages.confluent.io/maven/"

lazy val common = (project in file("./common"))
  .settings(
    name := "common",
    libraryDependencies := Seq(
      kafka,
      curator % "test",
      scalaLogging % "test",
      logback % "test",
      scalaTest % "test"
    )
  )

lazy val `kafka-client` = (project in file("./kafka-client"))
  .settings(Common.settings: _*)
  .settings(
    name := "kafka-client",
    libraryDependencies := Seq(
      kafka,
      curator % "test",
      scalaLogging % "test",
      logback % "test",
      scalaTest % "test"
    ),
    parallelExecution in Test := false
  )
  .dependsOn(common)

lazy val `reactive-kafka` = (project in file("./reactive-kafka"))
  .settings(Common.settings: _*)
  .settings(
    name := "reactive-kafka",
    libraryDependencies := Seq(
      akkaActor,
      akkaStream,
      reactiveKafka,
      kafka,
      curator % "test",
      scalaLogging % "test",
      logback % "test",
      scalaTest % "test"
    ),
    parallelExecution in Test := false
  )
  .dependsOn(common)

lazy val root = (project in file(".")).
  aggregate(`kafka-client`, `reactive-kafka`, common)
