package l_9

// Scala 2.13
import scala.collection.parallel.CollectionConverters._

object ParallelCollections extends App {
  (0 to 10).foreach(println)
  println()

  (0 to 10)
    .par
    .foreach(println)
}
