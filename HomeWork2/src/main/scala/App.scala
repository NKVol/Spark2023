package ru.otus.spark

object App extends SparkSessionWrapper{
  def main(args: Array[String]) = {
    /*
    Задание 1:
    Загрузить данные в первый DataFrame из файла с фактическими данными поездок в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
    Загрузить данные во второй DataFrame из файла со справочными данными поездок в csv (src/main/resources/data/taxi_zones.csv)
    С помощью DSL построить таблицу, которая покажет какие районы самые популярные для заказов.
    Результат вывести на экран и записать в файл Паркет.
    Результат: В консоли должны появиться данные с результирующей таблицей, в файловой системе должен появиться файл.
    Решение оформить в github gist.
     */
    DataFrameOperation.showDataFrames()
    DataFrameOperation.saveTablePopularBorough()
    /*
    Задание 2:
    Загрузить данные в RDD из файла с фактическими данными поездок
    в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
    С помощью lambda построить таблицу, которая покажет в какое время происходит больше всего вызовов.
    Результат вывести на экран и в txt файл c пробелами.
    Результат: В консоли должны появиться данные с результирующей таблицей, в файловой системе должен появиться файл.
    Решение оформить в github gist.
     */
    RDDOperation.showRDDData()
    RDDOperation.saveRDDTopTimeTextFile()

    /*
    Задание 3:
    Загрузить данные в DataSet из файла с фактическими данными поездок в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
    С помощью DSL и lambda построить таблицу, которая покажет. Как происходит распределение поездок по дистанции.
    Результат вывести на экран и записать в СУБД PostgreSQL (файл docker-compose в корневой папке проекта).
    Для записи в базу данных необходимо продумать и также приложить init SQL файл со структурой.
    (Пример: можно построить витрину со следующими колонками: общее количество поездок, среднее расстояние,
    среднеквадратическое отклонение, минимальное и максимальное расстояние)
     */
    DataFrameOperation.getBoroughInfo().show()
    DataFrameOperation.writeBoroughInfoIntoPG()
  }

}
