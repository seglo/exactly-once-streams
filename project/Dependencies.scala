import Versions._
import sbt._

object Dependencies {

  implicit class Exclude(module: ModuleID) {
    def log4jExclude: ModuleID =
      module excludeAll(ExclusionRule("log4j"))

    def driverExclusions: ModuleID =
      module.log4jExclude.exclude("com.google.guava", "guava")
        .excludeAll(ExclusionRule("org.slf4j"))
  }

  val kafkaStreams = "org.apache.kafka"                % "kafka-streams"     % KafkaVersion
  val scalaLogging = "com.typesafe.scala-logging"     %% "scala-logging"     % ScalaLoggingVersion
  val logback = "ch.qos.logback"                       % "logback-classic"   % LogbackVersion
  val kafka = "org.apache.kafka"                      %% "kafka"             % KafkaVersion
  val curator = "org.apache.curator"                   % "curator-test"      % CuratorVersion
  val scalaTest = "org.scalatest"                     %% "scalatest"         % ScalaTestVersion
//  val minitest = "io.monix"                           %% "minitest"          % MinitestVersion
//  val minitestLaws = "io.monix"                       %% "minitest-laws"     % MinitestVersion
  val akkaActor = "com.typesafe.akka"                 %% "akka-actor"        % AkkaVersion
  val akkaTestkit = "com.typesafe.akka"               %% "akka-testkit"      % AkkaVersion
  val akkaStream = "com.typesafe.akka"                %% "akka-stream"       % AkkaVersion
}
