/*
Задача:
В урне 3 белых и 3 черных шара. Из урны дважды вынимают по одному шару, не возвращая их обратно.
Найти вероятность появления белого шара
PS: чем больше будет количество опытов в пункте 2, тем ближе будет результат моделирования к аналитическому решению
 */

package scala
object HWRun extends App {
  //создать класс с моделированием эксперимента, в нем должна быть коллекция (List)
  // моделирующая урну с шариками (1 - белый шарик, 0 - черный шарик)
  case class Experiment(box: List[Int])
  val experiment: Experiment = Experiment(List(1, 1, 1, 0, 0, 0))

  //функция случайного выбора 2х шариков без возвращения (scala.util.Random),
  // возвращать эта функция должна true (если был выбран белый шар) и false (в противном случае)
  def random_choice(x: Experiment): Boolean = {
    scala.util.Random.shuffle(x.box).take(2).contains(1)
  }

  //создать коллекцию обьектов этих классов, скажем 10000 элементов
  val experiment_collection = List.fill(10000){experiment}

  //и провести этот эксперимент (функция map)
  val experiment_result = experiment_collection.map(random_choice).filter(x => x).length / 10000f
  println(f"${experiment_result}%.4f")
}
