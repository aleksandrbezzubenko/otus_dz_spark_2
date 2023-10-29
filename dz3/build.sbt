
lazy val mainSettings = Seq(
  name := "dz3",
  version := "0.1.0",
  organization := "org.example",
  scalaVersion := "2.12.15"
)

val testcontainersScalaVersion = "0.40.0"
lazy val parser = (project in file(".")).
  settings(mainSettings: _*).
  settings {
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.0.1",
      "org.postgresql" % "postgresql" % "42.3.0"  from "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.0/postgresql-42.3.0.jar",
      "org.scalatest" %% "scalatest" % "3.2.2",
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion from "https://repo1.maven.org/maven2/com/dimafeng/testcontainers-scala-scalatest_2.12/0.40.0/testcontainers-scala-scalatest_2.12-0.40.0.jar",
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersScalaVersion from "https://repo1.maven.org/maven2/com/dimafeng/testcontainers-scala-postgresql_2.12/0.40.0/testcontainers-scala-postgresql_2.12-0.40.0.jar"
    )
  }
