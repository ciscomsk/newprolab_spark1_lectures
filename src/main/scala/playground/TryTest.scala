package playground

import scala.util.Try

object TryTest extends App {
  val res1 = Try(throw new RuntimeException("exception")).toOption
  println(res1) // None

  val res2 = Try(throw new RuntimeException("exception")).toEither
  println(res2) // Left(java.lang.RuntimeException: exception)
}
