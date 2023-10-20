package ru.otus.spark
import org.apache.spark.sql.{DataFrame}

object DataFrameCreate extends SparkSessionWrapper {
  def fromFileParquet(): DataFrame = spark.read.parquet("src/main/resources/data/yellow_taxi_jan_25_2018")

  def fromFileCsv(): DataFrame = spark.read
    .option("delimiter", ",")
    .option("header", true)
    .option("inferSchema", true)
    .csv("src/main/resources/data/taxi_zones.csv")

  def toFileParquet(x: DataFrame): Unit = {
    x.write.mode("overwrite").parquet("src/main/resources/data/topBorough")
  }

}
