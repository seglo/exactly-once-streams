import Dependencies._
import Versions._
import sbt.ExclusionRule

lazy val `exactly-once-streams` = project.in(file("."))
  .settings(
    name := "exactly-once-streams",
    version := "1.0",
    scalaVersion := Scala_2_12_Version,
    libraryDependencies := Seq(
      akkaActor,
      akkaStream,
      kafka excludeAll(ExclusionRule("org.slf4j", "slf4j-log4j12"), ExclusionRule("org.apache.zookeeper", "zookeeper")),
      curator % "test",
      scalaLogging % "test",
      logback % "test",
      akkaTestkit % "test",
      scalaTest % "test"
    ),
    parallelExecution in Test := false
  )
