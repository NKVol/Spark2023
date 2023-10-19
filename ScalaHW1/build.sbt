import dependencies.library

ThisBuild / organization := "NG"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"
scalacOptions ++=Seq("-target:jvm-11")

lazy val root = (project in file("."))
  .settings(
    name := "ScalaHW1",
    libraryDependencies ++= Seq(
      library.sparkSql
    ),
    //Ошибки при сборке убирает для зависимостей
    assembly / assemblyMergeStrategy := {
      case PathList("META_INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
