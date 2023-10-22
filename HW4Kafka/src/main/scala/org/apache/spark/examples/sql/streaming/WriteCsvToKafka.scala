package ru.otus.spark
import org.apache.spark.examples.sql.streaming.SparkSessionWrapper
import org.apache.spark.sql.DataFrame

object WriteCsvToKafka extends SparkSessionWrapper {

  def fromFileCsv(): DataFrame = spark.read
    .option("delimiter", ",")
    .option("header", true)
    .option("inferSchema", true)
    .csv("src/main/resources/bestsellers with categories.csv")


  def writeCsvToKafka(): Unit = {
    val df: DataFrame = fromFileCsv()
    val js = df.toJSON
    js.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "books")
      .save()
  }
}
