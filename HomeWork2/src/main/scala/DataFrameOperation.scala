package ru.otus.spark
import org.apache.spark.sql.DataFrame
import DataFrameCreate._
import org.apache.spark.sql.functions._

object DataFrameOperation extends SparkSessionWrapper {
  import spark.implicits._

  //Загрузить данные в первый DataFrame из файла с фактическими данными поездок
  // в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
  val parqDf: DataFrame = fromFileParquet()
  //Загрузить данные во второй DataFrame из файла со справочными данными поездок
  // в csv (src/main/resources/data/taxi_zones.csv)
  val csvDf: DataFrame = fromFileCsv()

  def showDataFrames(): Unit = {
    parqDf.printSchema()
    parqDf.cache()
    parqDf.head

    csvDf.printSchema()
    csvDf.cache()
    csvDf.head

    //посмотрим на данные
    parqDf.show()
    csvDf.show()
  }

  def getTopBorough(): DataFrame = {
    //С помощью DSL построить таблицу, которая покажет какие районы самые популярные для заказов.
    parqDf
      .join(csvDf, parqDf("DOLocationID") === csvDf("LocationID"))
      .groupBy("Borough")
      .agg(
        count("VendorID").as("tripsAmount")
      )
      .orderBy(col("tripsAmount").desc)
      .select("Borough", "tripsAmount")
  }


  def saveTablePopularBorough() : Unit = {

    //Выбираем самые популярные районы
    val topBorough: DataFrame = getTopBorough()
    //выводим на экран
    topBorough.show()
    //сохраняем в файл
    toFileParquet(topBorough)
  }







}
