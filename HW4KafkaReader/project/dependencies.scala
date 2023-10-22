import sbt._

object dependencies {
  val library: Object {
    val sparkSql: ModuleID
    val postgresSql: ModuleID
    val sparkStreaming: ModuleID
  } = new {
    object Version {
      lazy val spark = "3.3.0"
    }

    val sparkSql = "org.apache.spark" %% "spark-sql" % Version.spark
    val postgresSql = "org.postgresql" % "postgresql" % "42.5.4"
    val sparkStreaming = "org.apache.spark" % "spark-streaming_2.12" % Version.spark % "provided"
  }
}
