package l_2

class Point(xc: Int, yc: Int) {
  var x: Int = xc
  var y: Int = yc

  def move(dx: Int, dy: Int): Unit = {
    x += dx
    y += dy

    println(s"Point x location: $x")
    println(s"Point y location: $y")
  }
}

object App {
  def main(args: Array[String]): Unit = {
    val pt: Point = new Point(10, 20)

    println(s"Point x location: ${pt.x}")
    println(s"Point y location: ${pt.y}")
  }
}
