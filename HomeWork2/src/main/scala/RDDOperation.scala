package ru.otus.spark

import RDDCreate._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import java.io.{File, PrintWriter}
import java.sql.Timestamp

object RDDOperation extends SparkSessionWrapper {
  //Загрузить данные в RDD из файла с фактическими данными поездок
  //в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
  import spark.implicits._
  val parqRDD = fromFileParquetRdd()

  def showRDDData(): Unit = {
    println(parqRDD.first())
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  def saveRDDTopTimeTextFile(): Unit = {
    import spark.implicits._
    //С помощью lambda построить таблицу, которая покажет в какое время происходит больше всего вызовов.
    val x = parqRDD.map(_(1)).collect
      .map(f => Timestamp.valueOf(f.toString).getHours)
      .groupBy(f => f)
      .map(t => (t._1, t._2.length))
      .toSeq.sortWith(_._2 > _._2)

    x.foreach(tuple => println(tuple.productIterator.mkString(" ")))

    printToFile(new File("src/main/resources/data/yellow_taxi_top_hour.txt")) {
      p => x.foreach(tuple => p.println(tuple.productIterator.mkString(" ")))
    }
  }

}
