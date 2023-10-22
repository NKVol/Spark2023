package org.apache.spark.examples.sql.streaming
package ru.otus.spark

import org.apache.spark.sql.DataFrame

import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.spark.examples.sql.streaming.SparkSessionWrapper

object StructuredReadFromKafka extends SparkSessionWrapper {

  def toFileParquet(x: DataFrame): Unit = {
    x.write.mode("overwrite").parquet("src/main/resources/books")
  }

  def readFromKafka(): Unit = {
    /*
    * Написать приложение, которое будет читать из темы books данные, сериализованные в JSON
     */

    val spark = SparkSession
      .builder
      .appName("StructuredReadFromKafka")
      .getOrCreate()

    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "books")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    /*
    * отфильтровывать записи с рейтингом меньше 4 и сохранять результат на диск в формате Parquet.
     */
    val lowRating = lines.map(_(3).toFloat).filter(_ < 4).toDF
    toFileParquet(lowRating)

  }
}
