lazy val mainSettings = Seq(
  name := "dz6",
  version := "0.1.0",
  organization := "com.example",
  scalaVersion := "2.12.8"
)

val sparkVersion = "2.4.7"
lazy val parser = (project in file(".")).
  settings(mainSettings: _*).
  settings {
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0",
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
      "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
      "org.scalatest" %% "scalatest" % "3.2.2",
      "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0" % "test"
    )
  }
