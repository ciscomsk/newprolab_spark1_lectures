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


"boo:and:foo".split("o", -1)
"boo:and:foo".split("o", 0)