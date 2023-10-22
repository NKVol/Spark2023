import dependencies.library

ThisBuild / organization := "NG"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.12"
scalacOptions ++=Seq("-target:jvm-11")

lazy val root = (project in file("."))
  .settings(
    name := "SparkHW4",
    libraryDependencies ++= Seq(
      library.postgresSql,
      library.sparkSql,
      library.sparkStreaming,
    ),
  )