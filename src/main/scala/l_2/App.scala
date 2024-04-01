package l_2

import l_2.ScalaTutorial.Point

object App {
  def main(args: Array[String]): Unit = {
    val pt: Point = new Point(10, 20)

    println(s"Point x location: ${pt.x}")
    println(s"Point y location: ${pt.y}")
  }
}
