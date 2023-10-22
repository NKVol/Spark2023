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
    val checkpointLocation =  "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("StructuredReadFromKafka")
      .getOrCreate()

    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.server", "localhost:9092")
      .option("topic", "books")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    // Generate running word count
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}
