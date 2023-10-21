package ru.otus.spark
import org.apache.spark.sql.DataFrame
import DataFrameCreate._
import org.apache.spark.sql.functions._

object DataFrameOperation extends SparkSessionWrapper {

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

  def getBoroughInfo(): DataFrame = {
    /*
      Загрузить данные в DataSet из файла с фактическими данными поездок в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
        С помощью DSL и lambda построить таблицу, которая покажет. Как происходит распределение поездок по дистанции.
        Результат вывести на экран и записать в СУБД PostgreSQL (файл docker-compose в корневой папке проекта).
        Для записи в базу данных необходимо продумать и также приложить init SQL файл со структурой.
        (Пример: можно построить витрину со следующими колонками: общее количество поездок, среднее расстояние,
        среднеквадратическое отклонение, минимальное и максимальное расстояние)
        CREATE TABLE taxi_stat (
          trips_amount integer,
          avg_distance float,
          standard_deviation float,
          min_distance float,
          max_distance float
        );
     */

    //С помощью DSL построить таблицу, которая покажет какие районы самые популярные для заказов.
    return parqDf
      .agg(
        count("VendorID").as("tripsAmount"),
        avg("trip_distance").as("avgDistance"),
        min("trip_distance").as("minDistance"),
        max("trip_distance").as("maxDistance"),
      ).toDF()

  }

  def writeBoroughInfoIntoPG(): Unit = {
    val df: DataFrame = getBoroughInfo()
    df.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://0.0.0.0:5432/otus")
    .option("dbtable", "public.taxi_stat")
    .option("user", "docker")
    .option("password", "docker")
    .save()
  }






}
