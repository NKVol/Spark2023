package org.apache.spark.examples.sql.streaming
import org.apache.spark.examples.sql.streaming.ru.otus.spark.StructuredReadFromKafka._

object App extends SparkSessionWrapper{
  def main(args: Array[String]) = {
    /*
      Задание 1:
      Написать приложение, которое будет читать CSV файл,
      сериализовать его в JSON и отправлять в тему books.
    */

    readFromKafka()
  }

}
