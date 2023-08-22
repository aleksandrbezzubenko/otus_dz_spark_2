
object Main extends App {

  def duplicateN[A](n: Int, list: List[A]):List[A] = {
    list.flatMap { e => List.fill(n)(e) }
  }

  val experiment = new Experiment(List(true, true, true, false, false, false))
  val experiments10000 = duplicateN(10000, List(experiment))
  val takedBalls = experiments10000.map(e => e.takeBalls())

  println(takedBalls.count(a => a).toDouble/takedBalls.length)
}
