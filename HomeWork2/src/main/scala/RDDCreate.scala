package ru.otus.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


object RDDCreate extends SparkSessionWrapper {
  def fromFileParquetRdd(): RDD[Row] = spark.read.parquet("src/main/resources/data/yellow_taxi_jan_25_2018").rdd



}
