"String".map(_ + 1)
"String".map(_ + 1).map(_.toChar)

"String".map(el => Seq(el + 1))
"String".flatMap(el => Seq(el + 1))

"String".toVector
List("String").map(_.toVector)
List("String").flatMap(_.toVector)

"String".toLowerCase()
List("String").map(_.toLowerCase())
List("String").map(_.toLowerCase()).flatten
List("String1", "String2").map(_.toLowerCase())
List("String1", "String2").map(_.toLowerCase()).flatten


"boo:and:foo".split("o", 2)
"boo:and:foo".split("o", 0)
"boo:and:foo".split("o", -1)
