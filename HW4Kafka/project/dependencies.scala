import sbt._

object dependencies {
  val library: Object {
    val sparkSql: ModuleID
    val postgresSql: ModuleID
    val sparkStreaming: ModuleID
    val sparkStreamingKafka: ModuleID
    val sparkSqlKafka: ModuleID
  } = new {
    object Version {
      lazy val spark = "3.1.2"
    }

    val sparkSql = "org.apache.spark" %% "spark-sql" % Version.spark
    val postgresSql = "org.postgresql" % "postgresql" % "42.5.4"
    val sparkStreaming = "org.apache.spark" % "spark-streaming_2.12" % Version.spark % "provided"
    val sparkStreamingKafka =  "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % Version.spark
    val sparkSqlKafka =  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % Version.spark
  }
}
