package org.apache.spark.examples.sql.streaming
import org.apache.spark.examples.sql.streaming.ru.otus.spark.StructuredReadFromKafka._

object App extends SparkSessionWrapper{
  def main(args: Array[String]) = {
    readFromKafka()
  }

}
