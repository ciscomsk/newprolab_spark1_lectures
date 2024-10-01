package l_2

import scala.collection.{MapView, mutable}
import scala.language.{implicitConversions, postfixOps}
import scala.util.{Failure, Random, Success, Try}

object ScalaTutorial extends App {
  /** Литералы */
  val int: Int = 1
  val str: String = "Some words"
  val bool: Boolean = true

  /** Выражения */
  val expr1: Int = 1 + 3
  val expr2: String = "Some" ++ " words"

  /**
   * 1. значение блока равно значению последнего выражения в блоке
   * 2. return - не используется
   */
  val c: Int = {
    val a: Int = 11
    a + 42
  }
  println(s"c: $c")
  println()

  /** Unit, литерал - () - выражение, которое не возвращает значение = Java void */
  def printer(s: String): Unit = println(s)

  /** val = value - Immutable */
  val thisIsVal: Int = 1
//  thisIsVal = 2 // compile time err - reassignment to val

  /** var = variable - Mutable */
  var thisIsVar: Int = 1
  thisIsVar = 2 // ok


  /** в Scala все операторы являются методами */
  val v1: String = "Some words".toUpperCase()
  println(v1)

  /** Постфиксная нотация - используется для методов без аргументов */
  /** без ";" не работает т.к. парсер считает 'println(v2)' - аргументом -> test.sc */
  val v2: String = "Some words" toUpperCase; // постфиксная нотация
  println(v2)
  println()

  /**
   * Инфиксная нотация - используется для методов с 1-м аргументом
   * инфиксная нотация полезна при написании DSL: select * from table1 => select.(*).from(table1)
   */
  val v3: String = "Some" concat " words" // инфиксная нотация
  println(v3)
  // =
  val v4: String = "Some" ++ " words" // инфиксная нотация
  println(v4)
  // =
  val v5: String = "Some".++(" words")
  println(v5)
  println()


  /** Анонимные функции */
  val f1: Int => Int = (x: Int) => x + 1

  /** Методы */
  def incr(x: Int): Int = x + 1

  def sum(a: Int, b: Int): Int = a + b
  val res1: Int = sum(3, 4)
  println(res1)
  println()

  /** HOF = higher order function */
  def add(x: Int): Int => Int = y => x + y
  val addOne: Int => Int = add(1)
  println(s"addOne: $addOne")

  val res2: Int = addOne(2)
  println(s"res2: $res2")
  println()

  val intList: List[Int] = List(1, 2, 3)

  val res6: List[Int] = intList.map(incr)
  println(res6)
  // =
  val res3: List[Int] = intList.map((x: Int) => x + 1)
  println(res3)
  // =
  val res4: List[Int] = intList.map(x => x + 1)
  println(res4)
  // =
  val res5: List[Int] = intList.map(_ + 1)
  println(res5)
  println()


  /** Функции могут быть определены как def или val */
  def foo1(): String = "foo"
  // =
  val foo2: () => String = () => "foo"
  println(s"foo1:${foo1()}, foo2:${foo2()}")
  println()

  /**
   * val - вычисляется 1 раз при появлении в области видимости
   * lazy val - вычисляется 1 раз при первом обращении к переменной
   * def = define - выполняется при каждом вызове
   */

  class TestClass {
    val rnd: Random = new Random()

    val `val`: Int = callWrapper("val")
    lazy val lazyVal: Int = callWrapper("lazy val")
    def `def`(): Int = callWrapper("def()")

    def callWrapper(objName: String): Int = {
      println(s">> '$objName' init")
      rnd.nextInt()
    }
  }

  val test1: TestClass = new TestClass
  println(">> test1 created")
  println()

  /** Call by value - аргумент будет рассчитан 1 раз перед вызовом функцию */
  def repeatByValue(obj: Int): Unit = (1 to 3).foreach(_ => println(obj))
  println("Call by Value:")
  repeatByValue(test1.`val`)
  repeatByValue(test1.lazyVal)
  repeatByValue(test1.`def`())
  println()

  val test2: TestClass = new TestClass
  println(">> test2 created")
  println()

  /** Call by name - аргумент рассчитывается каждый раз при обращении к нему в теле функции */
  def repeatByName(obj: => Int): Unit = (1 to 3).foreach(_ => println(obj))
  println("Call by Name:")
  repeatByName(test2.`val`)
  repeatByName(test2.lazyVal)
  repeatByName(test2.`def`())
  println()

  /**
   * Short circuit семантика:
   * если a = false, b - можно не рассчитывать (b: => Boolean) - оптимизация
   */
  def and(a: Boolean, b: => Boolean): Boolean = {
    if (!a) false
    else a && b
  }


  /** Управляющие конструкции */
  /** if - это выражение => возвращает значение */
  val x: Int = 1
  val res7: Unit = if (x < 20) println("This is if statement")

  val whichOne: String = if (false) "Not that one" else "This one"
  println(whichOne)
  println()

  val howMuch1: String = if (x < 20) "some" else "many"
  println(howMuch1)
  // =
  val howMuch2: String =
    if (x < 20) {
      "some"
    } else {
      "many"
    }
  println(howMuch2)
  println()

  /** for - обход коллекций */
  // v1
//  for (a <- 1 to 3) println(s"Value of a: $a")

  // v2
  (1 to 3).foreach(el => println(s"value of el: $el"))
  println()

  val range: Range.Inclusive = 1 to 3 // 1 to 3 = 1.to(3) - пример инфиксной нотации
  for (x <- range) {
    println(x + 2)
  }
  println()

  /** for-comprehension */
  val res8: Seq[Int] =
    for {
      a <- 1 to 5 // коллекция 1
      b <- 1 to 5 // коллекция 2
      if a + b < 6 // условие
    } yield a + b // генератор
  // =
  (1 to 5).flatMap { a =>
    (1 to 5)
      .withFilter(b => a + b < 6)
      .map(b => a + b)
  }
  println(s"for-comprehension: $res8")
  println()


  /** Pattern matching - Java switch на "стероидах" */
  val any: Any = 10

  val res9: Any =
    any match { // partially applied function
      case 1 => "one"
      case "two" => 2
      case y: Int => s"$y is scala.Int"
      case _ => "unknown"
    }
  println(res9)
  println()

  def test(val1: Int, val2: Int): (Int, Int) = (val1, val2)

  /** Распаковка - работает с помощью unapply */
  val (a, b) = test(1, 2)
  println(s"a = $a, b = $b")
  println()


  /** Collections API */
  /** List - эффект множества значений */
  println("__List__")

  val fruits1: List[String] = List("apple", "banana", "pear")
  println(fruits1)
  // =
  val fruits2: List[String] = "apple" :: ("banana" :: ("pear" :: Nil)) // скобки только для удобства понимания
  // :: - правоассоциативный метод - "pear" :: Nil = Nil.::("pear")
  println(fruits2)
  println()

  val filledList: List[String] = List.fill(3)("apple")
  println(s"List.fill(3)(\"apple\"): $filledList")

  val mappedList: List[String] = fruits1.map(el => s"This is $el")
  println(s"fruits1.map(el => s\"This is $$el\"): $mappedList")

  val contains: Boolean = fruits1.contains("apple")
  println(s"fruits1.contains(\"apple\"): $contains")

  val head1: String = fruits1.head
  // =
  val head2: String = fruits1(0)
  println(s"fruits1.head: $head1")

  val take: List[String] = fruits1.take(2)
  println(s"fruits1.take(2): $take")

  val filterEnds: List[String] = fruits1.filter(_.endsWith("e"))
  println(s"fruits1.filter(_.endsWith(\"e\")): $filterEnds")

  val existStarts: Boolean = fruits1.exists(_.startsWith("b"))
  println(s"fruits1.exists(_.startsWith(\"b\")): $existStarts")

  val distinct: List[String] = fruits1.distinct
  println(s"fruits1.distinct: $distinct")

  val size: Int = fruits1.size
  println(s"fruits1.size: $size")
  println()

  /** Aggregation */
  val listTup2: List[(String, Int)] = List(("apple", 1), ("apple", 2), ("apple", 3), ("orange", 2))

  val groupBy: Map[String, List[(String, Int)]] = listTup2.groupBy(_._1)
  println(s"listTup2.groupBy(_._1): $groupBy")

  // Scala 2.12
//  val mapValues: MapView[String, Int] =
//    listTup2
//      .groupBy(_._1)
//      .mapValues(list => list.map { case (_, num) => num }.sum)

  // Scala 2.13
  val mapValues: MapView[String, Int] =
    listTup2
      .groupBy(_._1)
      .view
      .mapValues(list => list.map { case (_, num) => num }.sum)

  println(s"mapView: $mapValues")
  println(s"mapView.toMap: ${mapValues.toMap}")
  println()

  val intList2: List[Int] = List(1, 7, 5, 9, 3)

  val reduce: Int = intList2.reduce { (el1, el2) => if (el1 > el2) el1 else el2 } // = Math.max
  println(s"reduce: $reduce")

  val reduceOption: Option[Int] = List.empty[Int].reduceOption(_ + _)
  println(s"reduceOption: $reduceOption")

  val fold: (Double, Int) = intList2.foldLeft(0.0, 0) { (acc, el) => (acc._1 + el, acc._2 + 1) }
  println(s"fold: $fold")
  println(fold._1 / fold._2) // mean
  println()

  /** map: A => B */
  println("_map_: A => B")

  val map2: List[Int] = intList.map(_ + 1)
  println(map2)
  val emptyList: List[Int] = List.empty[Int].map(_ + 1)
  println(emptyList)
  println()

  /** flatten: G[F[A]] => G[A] - распаковывает внутренние коллекции (монады) */
  println("_flatten_: G[F[A]] => G[A]")

  val originalList: List[List[Int]] = List(List(1), List(2, 3), List(4, 5), Nil)
  val flattenedList: List[Int] = originalList.flatten
  println(s"originalList: $originalList")
  println(s"flattenedList: $flattenedList")
  println()

  /**
   * flatMap: A => F[B]
   * flatMap = map + flatten
   */
  println("_flatMap_: A => F[B]")

  val flatMap: List[Int] = intList.flatMap(el => List(el, el, el))
  println(flatMap)
  // =
  val mapFlatten: List[Int] = intList.map(el => List(el, el, el)).flatten
  println(mapFlatten)
  println()

  /** foreach: A => Unit */
  println("_foreach_: A => Unit")

  intList.foreach(println)
  println()


  /** Map */
  println("__Map__")

  val fruitsMap: Map[String, Int] = Map("apple" -> 2, "banana" -> 1, "pear" -> 10)
  println(fruitsMap)

  /** unsafe get value */
//  val unsafeValue: Int = fruitsMap("peach") // err - key not found

  /** safe get value - v1 - getOrElse: A */
  val safeValue1: Int = fruitsMap.getOrElse("peach", 10)
  println(safeValue1)

  /** safe get value - v2 - get: Option[A] */
  val safeValue2: Option[Int] = fruitsMap.get("peach")
  println(safeValue2)
  println()

  // "apple" -> 2 = ("apple", 2) - сахар для кортежа
  val list2Map: Map[String, Int] = List("apple" -> 2, "banana" -> 1, "pear" -> 10).toMap
  println(s"list2Map: $list2Map")

  val map2List: List[(String, Int)] = fruitsMap.toList
  println(s"map2List: $map2List")
  println()

  val colors: Map[String, String] = Map("red" -> "#FF0000", "azure" -> "#F0FFFF")
  println(s"colors.keys: ${colors.keys}")
  println(s"colors.values: ${colors.values}")
  println(s"colors.isEmpty: ${colors.isEmpty}")
  println()

  // Immutable map - updateD, result type = Map
  val colorsImmutable: Map[String, String] = colors.updated("black", "#000000")
  println(s"colors before updateD: $colors")
  println(s"colorsImmutable: $colorsImmutable")
  println(s"colors after updateD: $colors")
  println()

  // Mutable map - update, result type = Unit
  val colorsMutable: mutable.Map[String, String] = mutable.Map("red" -> "#FF0000", "azure" -> "#F0FFFF")
  println(s"colorsMutable original: $colorsMutable")
  val _: Unit = colorsMutable.update("black", "#000000")
  println(s"colorsMutable after update: $colorsMutable")
  println()


  /** Set */
  println("__Set__")

  val set: Set[String] = Set("apple", "banana", "banana", "banana", "pear")
  println(set)
  println()


  /** Tuple - элементы могут иметь разные типы */
  println("__Tuple__")

  val t3: (Int, String, Double) = (1, "hello", 3.0)
  println(t3)

  val t4: (Int, Int, Int, Int) = (4, 3, 2, 1)
  println(t4)

  val t4Sum: Int = t4._1 + t4._2 + t4._3 + t4._4
  println(t4Sum)
  println()

  var tNull: (Int, String, Double) = null
  println(s"tNull: $tNull")

//  tNull = (1, 2, 3.0) // err - type mismatch
  tNull = (1, "2", 3.0) // ok
  println(s"tNull: $tNull")
  println()


  /** Option = Some(value)/None - эффект возможного отсутствия значения = коллекция с 0/1 элементом */
  println("__Option__")

  val optSome: Option[Int] = Some(3)
  println(s"optSome: $optSome")
  val optNone: Option[Int] = None // = Option.empty[Int]
  println(s"optNone: $optNone")
  println()

  val aVal: Int = null.asInstanceOf[Int] // = 0 - в Scala anyVal классы не могут принимать значение null (~примитивы в Java)
  println(s"aVal: $aVal")

  val bVal: java.lang.Integer = null // = null - в Java numeric классы могут принимать значение null, примитивы - не могут
  println(s"bVal: $bVal")

  val cVal: Int = aVal + bVal
  println(s"cVal: $cVal") // = 0
  println()

  val d1Val: Int = bVal
  println(s"d1Val: $d1Val") // = 0

  val d2Val: java.lang.Integer = bVal
  println(s"d2Val: $d2Val") // = null
  println()

  val nonEmptyOption: Option[Int] = Some(5)
  println(s"nonEmptyOption.getOrElse(10): ${nonEmptyOption.getOrElse(10)}")
  println(s"nonEmptyOption.map(_ + 1): ${nonEmptyOption.map(_ + 1)}")
  println(s"nonEmptyOption.toList: ${nonEmptyOption.toList}")
  println()

  val emptyOption: Option[Int] = None
  println(s"emptyOption.getOrElse(10): ${emptyOption.getOrElse(10)}")
  println(s"emptyOption.map(_ + 1): ${emptyOption.map(_ + 1)}")
  println(s"emptyOption.toList: ${emptyOption.toList}")
  println()

  val listWithOption: List[Option[Int]] = List(Some(1), None, Some(2))
  println(s"listWithOption: $listWithOption")

  val flattenedListWithOptions: List[Int] = listWithOption.flatten
  println(s"flattenedListWithOption: $flattenedListWithOptions")
  println()


  /** Try = Success(value)/Failure(exception) - эффект возможной ошибки = коллекция с 0/1 элементом */
  println("__Try__")

  val iHopeItsNumbers: List[String] = List("1", "2", "banana", "4")
  println(iHopeItsNumbers)
  println()

//  val err: Int = "a".toInt // err - NumberFormatException

  /** try - Scala style */
  // toInt = java.lang.Integer.parseInt()
  def toOptInt(in: String): Option[Int] = Try(in.trim.toInt).toOption

  /** try - Java style */
  def toOptIntJava(in: String): Option[Int] = {
    try {
      Some(in.trim.toInt)
    } catch {
      case _: NumberFormatException => None
      case _: Throwable => None // Throwable - перехват всех ошибок
    }
  }

  val res10: List[Option[Int]] = iHopeItsNumbers.map(toOptInt)
  println(s"iHopeItsNumbers.map(toOptInt): $res10")

  val res11: List[Int] = iHopeItsNumbers.flatMap(toOptInt)
  println(s"iHopeItsNumbers.flatMap(toOptInt): $res11")

  val res12: Int = res11.sum
  println(res12)
  println()

  val try1: AnyVal =
    try {
//      "a".toInt
      5 / 0
    } catch {
      case _: NumberFormatException => println("Bad input string")
      case ex: Throwable => println(ex.toString) // Throwable - перехват всех ошибок
      case _ => println("something is wrong")
    }
  println(s"try1: $try1")
  println()

  val try2: Try[Int] = Try("a".toInt)
  println(s"try2: $try2")
  println()

  /** safe get value - v1 - match */
  val tryRes1: Int =
    try2 match {
      case Failure(_) => 0
      case Success(value) => value
    }
  println(tryRes1)

  /** safe get value - v2 - getOrElse */
  val tryRes2: Int = try2.getOrElse(0)
  println(tryRes2)
  println()


  /** Class */
  println("__Class__")

  class Point(xc: Int, yc: Int) { // (xc: Int, yc: Int) - конструктор по умолчанию
    // поля = атрибуты
    var x: Int = xc
    var y: Int = yc

    // методы
    def move(dx: Int, dy: Int): Unit = {
      x += dx
      y += dy

      println(s"Point x location: $x")
      println(s"Point y location: $y")
    }
  }

  val pt: Point = new Point(1, 1)
  pt.move(10, 10)
  println()

  class LinkedList() { // конструктор по умолчанию
    var head: java.lang.Integer = null
    var tail: List[Int] = null

    def isEmpty: Boolean = tail != null

    def this(head: Int) = { // дополнительный конструктор 1
      this(); this.head = head
    }

    def this(head: Int, tail: List[Int]) = { // дополнительный конструктор 2
      this(head); this.tail = tail
    }
  }

  class P1(var s: String = "") {
    def pp(): Unit = println(s)
  }


  /** Case class */
  println("__Case class__")

  case class Person(name: String, age: Int)

  val garry: Person = Person("Garry", 22)
  println(garry)

  val goodOldGarry: Person = garry.copy(age = 60)
  println(goodOldGarry)
  println()

  val alice: Person = Person("Alice", 25)
  val bob: Person = Person("Bob", 32)
  val charlie: Person = Person("Charlie", 32)

  val personsList: Seq[Person] = List(alice, bob, charlie)

  // v1
//  for (person <- personsList) {
//    person match {
//      case Person("Alice", _) => println("Hi Alice!")
//      case Person("Bob", 32) => println("Hi Bob!")
//      case Person(name, age) => println(s"Age: $age years, name: $name")
//    }
//  }

  // v2
  personsList.foreach {
    case Person("Alice", _) => println("Hi Alice!")
    case Person("Bob", 32) => println("Hi Bob!")
    case Person(name, age) => println(s"Name: $name, age: $age years")
  }
  println()

  println("_unapply_:")
  val personBob: Person = Person("Bob", 32)
  val Person(name, age) = personBob
  println(s"name: $name, age: $age")

  val unapplyRes: Option[(String, Int)] = Person.unapply(personBob)
  println(unapplyRes)
  println()


  /** Object */
  println("__Object__")

  object ColorConfig {
    val options: List[String] = List("red", "green", "blue")
  }
  println(ColorConfig.options)
  println()


  /** Companion object: */
  println("__Companion object__")

  class MyString(s: String) {
    private var extraData: String = ""
    override def toString: String = s"$s$extraData"
  }

  object MyString {
    def apply(base: String): MyString = new MyString(base) // фабричный метод 1

    def apply(base: String, extra: String): MyString = { // фабричный метод 2
      val s: MyString = new MyString(base)
      s.extraData = extra
      s
    }
  }
  println(MyString("hello"))
  println(MyString("hello", " world"))
  println()


  /** Trait */
  println("__Trait__")

  /*
    package scala.collection.immutable

    trait Seq[+A]
      extends Iterable[A]
      with collection.Seq[A]
      with SeqOps[A, Seq, Seq[A]]
      with IterableFactoryDefaults[A, Seq]
   */

  trait Equal[A] {
    def isEqual(x: A): Boolean
    def isNotEqual(x: A): Boolean = !isEqual(x)
  }

  sealed trait Shape {
    def fullName: String
    def shapeName: String

    override def toString: String = fullName
  }

  trait Circle {
    val shapeName: String = "circle"
  }

  sealed trait Color {
    def colorName: String
  }

  trait Red extends Color {
    override def colorName: String = "red"
  }

  trait ColoredShape extends Shape with Color {
    override def fullName: String = s"$colorName $shapeName"
  }

  object RedCircle extends ColoredShape with Circle with Red
  println(RedCircle)
  println()


  /** Abstract class */
  println("__Abstract class__")

  abstract class Animal {
    def name: String
  }

  case class Cat(name: String) extends Animal
  case class Dog(name: String) extends Animal

  abstract class Fruit2 {
    val name: String

    def printName(): Unit = println(s"It's $name")
  }

  /** Анонимный класс - позволяет реализовать/расширить класс "на лету" */
  val apple2: Fruit2 = new Fruit2 {
    override val name: String = "apple"

    def newMethod(): Unit = printName()
  }
  apple2.printName() // = It's apple
  println()


  /** Наследование */
  println("__Наследование__")

  abstract class A {
    val message: String
  }

  class B extends A {
    override val message: String = "I'm an instance of class B"
  }

  trait C extends A {
    def loudMessage: String = message.toUpperCase
  }

  class D extends B with C

  val d: D = new D
  println(d.message) // = I'm an instance of class B
  println(d.loudMessage) // = I'M AN INSTANCE OF CLASS B
  println()

  class Complex(real: Double, imaginary: Double) {
    def re: Double = real
    def im: Double = imaginary

    override def toString: String = s"$re ${im}i"
  }


  /** Generic types */
  println("__Generic types__")

  class Stack[A] {
    private var elements: List[A] = Nil

    def push(x: A): Unit = elements = x :: elements

    def pick(): A = elements.head

    def pop(): A = {
      val currentTop: A = pick()
      elements = elements.tail
      currentTop
    }
  }

  val intStack: Stack[Int] = new Stack[Int]
  intStack.push(1)
  intStack.push(2)
  println(intStack.pop()) // = 2
  println(intStack.pop()) // = 1
  println()

  class Fruit

  class Apple extends Fruit {
    override def toString: String = "Apple"
  }

  class Banana extends Fruit {
    override def toString: String = "Banana"
  }

  val fruitStack: Stack[Fruit] = new Stack[Fruit]
  val apple: Fruit = new Apple
  val banana: Fruit = new Banana
  fruitStack.push(apple)
  fruitStack.push(banana)
  println(fruitStack.pop()) // = Banana
  println(fruitStack.pop()) // = Apple
  println()


  /** Модификаторы доступа */
  println("__Модификаторы доступа__")

  class AA {
    def publicMethod(): Unit = println("public")
    private def privateMethod(): Unit = println("private")
  }
  println()


  /** Implicit class */
  println("__Implicit class__")

  object Helper {
    implicit class StringExtended(str: String) {
      def sayItLoud(): Unit = println(s"${str.toUpperCase}!!!")
    }
  }

  import Helper.StringExtended
  "hi".sayItLoud()
  // =
  StringExtended("hi").sayItLoud()
  println()


  /** Implicit conversion */
  println("__Implicit conversion__")

  val flag: Boolean = true
//  val sum1: Int = flag + 1 // err - type mismatch

  implicit def bool2Int(b: Boolean): Int = if (b) 1 else 0
  val sum2: Int = flag + 1
  // =
  val sum3: Int = bool2Int(flag) + 1
  println(sum2)
  println(sum3)
  println()


  /** Implicit parameter */
  println("__Implicit parameter__")

  def usd2Rub(quantity: Double)(implicit rub2UsdRate: Double): Double = quantity * rub2UsdRate
  implicit val rub2UsdRate: Double = 90
  println(usd2Rub(10.0))
  println()


  /** Misc */
  println("__Misc__")

  val conv1: Long = 2.asInstanceOf[Long] // bad
  val conv2: Long = 2.toLong // good

  var counter: Int = 0
  while (counter < 5) {
    println(counter)
    counter += 1
  }
}
