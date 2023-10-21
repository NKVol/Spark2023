import sbt._

object dependencies {
  val library: Object {
    val sparkSql: ModuleID
    val postgresSql: ModuleID
  } = new {
    object Version {
      lazy val spark = "3.3.0"
    }

    val sparkSql = "org.apache.spark" %% "spark-sql" % Version.spark
    val postgresSql = "org.postgresql" % "postgresql" % "42.5.4"
  }
}
