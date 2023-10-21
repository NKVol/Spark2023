import dependencies.library

ThisBuild / organization := "NG"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"
scalacOptions ++=Seq("-target:jvm-11")

lazy val root = (project in file("."))
  .settings(
    name := "SparkHW2",
    libraryDependencies ++= Seq(
      library.postgresSql,
      library.sparkSql
    ),
    idePackagePrefix := Some("ru.otus.spark"),
  )