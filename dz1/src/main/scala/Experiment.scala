import scala.util.Random

class Experiment(container: List[Boolean]) {
  def takeBalls(): Boolean = {
    val random = Random.shuffle(container)
    if (random.take(2).contains(true)) true
    else false
  }
}
