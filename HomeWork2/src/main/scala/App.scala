/*
Задание 1:
Загрузить данные в первый DataFrame из файла с фактическими данными поездок в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
Загрузить данные во второй DataFrame из файла со справочными данными поездок в csv (src/main/resources/data/taxi_zones.csv)
С помощью DSL построить таблицу, которая покажет какие районы самые популярные для заказов.
Результат вывести на экран и записать в файл Паркет.
Результат: В консоли должны появиться данные с результирующей таблицей, в файловой системе должен появиться файл. Решение оформить в github gist.
 */
package ru.otus.spark

object App extends SparkSessionWrapper{
  def main(args: Array[String]) = {

    DataFrameOperation.showDataFrames()
    DataFrameOperation.saveTablePopularBorough()

  }

}
