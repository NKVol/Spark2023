import sbt._

object dependencies {
  val library: Object {
    val sparkSql: ModuleID
  } = new {
    object Version {
      lazy val spark = "3.3.0"
    }

    val sparkSql = "org.apache.spark" %% "spark-sql" % Version.spark
  }
}
