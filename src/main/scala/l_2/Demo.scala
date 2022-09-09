package l_2

import l_2.ScalaTutorial.Point

object Demo {
  def main(args: Array[String]): Unit = {
    val point: Point = new Point(10, 20)

    println(s"Point x location: ${point.x}")
    println(s"Point y location: ${point.y}")
  }

}
