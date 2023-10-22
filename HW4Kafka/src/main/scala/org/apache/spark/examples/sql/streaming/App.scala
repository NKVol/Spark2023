package org.apache.spark.examples.sql.streaming
import ru.otus.spark.WriteCsvToKafka._

object App extends SparkSessionWrapper{
  def main(args: Array[String]) = {
    /*
      Задание 1:

     */
    writeCsvToKafka()
  }

}
