lazy val akkaVersion = "2.5.3"
lazy val kafkaVersion = "0.11.0.0"

lazy val `exactly-once-akka-streams` = project.in(file("."))
  .settings(
    name := "exactly-once-akka-streams",
    version := "1.0",
    scalaVersion := "2.12.2",
    libraryDependencies := Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "org.scalatest" %% "scalatest" % "3.0.1" % "test"
    )
  )
