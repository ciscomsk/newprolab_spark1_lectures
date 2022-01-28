import scala.util.Try
import scala.util.Success
import scala.util.Failure

//val res1 = Try(Some("string").map(_.toBoolean).get) match {
//  case Success(x) => x
//  case Failure(_) =>
//}

Try(None.get)

val res2 = Try(Some("zzz").map(_.toBoolean).get) match {
  case Success(x) => x
  case Failure(_) => true
}
