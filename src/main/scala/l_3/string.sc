"String".map(_ + 1).map(_.toChar)
"String".map(_ + 1)
"String".flatMap(el => Seq(el + 1))

"String".toVector
List("String").map(_.toVector)

List("String").flatMap(_.toVector)

"String".toLowerCase()
List("String").map(_.toLowerCase())
List("String").map(_.toLowerCase()).flatten
List("String1", "String2").map(_.toLowerCase())
List("String1", "String2").map(_.toLowerCase()).flatten