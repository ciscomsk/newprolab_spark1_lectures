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
   * 1. Значение блока == значение последнего выражения в блоке
   * 2. return - не используется
   */
  val c: Int = {
    val a: Int = 11
    a + 42
  }

  /** Тип выражения, которое не возвращает значение - Unit () == java void */
  def printer(s: String): Unit = println(s)

  /** val == value - Immutable */
  val thisIsVal: Int = 1
//  thisIsVal = 2  // err - reassignment to val

  /** var == variable - Mutable */
  var thisIsVar: Int = 1
  thisIsVar = 2


  val v1: String = "Some words".toUpperCase()
  println(v1)

  /** Без ; не работает (т.к. считает println(v2) - аргументом) => test.sc */
  val v2: String = "Some words" toUpperCase;  // постфиксная нотация - для методов без аргументов
  println(v2)
  println()

  /** Инфиксная нотация полезна при написании dsl => select * from table1 == select.(*).from(table1) */
  /** В Scala все операторы являются методами */
  val v3: String = "Some" concat " words"  // инфиксная нотация - для методов с 1-м аргументом
  println(v3)

  val v4: String = "Some" ++ " words"
  println(v4)
  // ==
  val v5: String = "Some".++(" words")
  println(v5)
  println()

  def sum(a: Int, b: Int): Int = a + b
  val res1: Int = sum(3, 4)
  println(res1)
  println()


  /** Анонимные функции */
  val f1: Int => Int = (x: Int) => x + 1

  /** Методы */
  def incr(x: Int): Int = x + 1

  /** HOF - higher order function */
  def add(x: Int): Int => Int = y => x + y
  val addOne: Int => Int = add(1)
  println(addOne)

  val res2: Int = addOne(2)
  println(res2)
  println()

  val intList: List[Int] = List(1, 2, 3)

  val res3: List[Int] = intList.map((x: Int) => x + 1)
  println(res3)
  // ==
  val res4: List[Int] = intList.map(x => x + 1)
  println(res4)
  // ==
  val res5: List[Int] = intList.map(_ + 1)
  println(res5)
  // ==
  val res6: List[Int] = intList.map(incr)
  println(res6)
  println()



  /**
   * def - выполняется при каждом вызове
   * val - вычисляется 1 раз при появлении в области видимости
   * lazy val - вычисляется 1 раз при первом обращении
   */

  /** Функции могут быть определены как def или val */
  def foo(): String = "foo"
  // ==
  val bar: () => String = () => "bar"
  println(s"${foo()} ${bar()}")
  println()

  class TestClass {
    val rg: Random = new Random

    val `val`: Int = callWrapper("val")
    lazy val lazyVal: Int = callWrapper("lazy val")
    def `def`: Int = callWrapper("def")

    def callWrapper(objName: String): Int = {
      println(s">> Init '$objName'!")
      rg.nextInt()
    }
  }

  val t: TestClass = new TestClass
  println(">> t created")
  println()

  /** Call by name - obj будет рассчитан 1 раз перед вызовом функцию */
  def repeatByValue(obj: Int): Unit = (1 to 3).foreach(_ => println(obj))

  repeatByValue(t.`val`)
  repeatByValue(t.lazyVal)
  repeatByValue(t.`def`)
  println()

  /** Call by value - obj будет рассчитываться каждый раз при обращении к нему в теле функции */
  def repeatByName(obj: => Int): Unit = (1 to 3).foreach(_ => println(obj))
  repeatByName(t.`val`)
  repeatByName(t.lazyVal)
  repeatByName(t.`def`)
  println()

  /*
    Short circuit => если a == false, b - можно не рассчитывать => оптимизация
    b: => Boolean
   */
  def and(a: Boolean, b: => Boolean): Boolean = ???


  /** Управляющие конструкции **/
  /** If - это выражение => возвращает значение */
  val x: Int = 1
  val res7: Unit = if (x < 20) println("This is if statement")

  val whichOne: String = if (false) "Not that one" else "This one"
  println(whichOne)
  println()

  val howMuch1: String = if (x < 20) "some" else "many"
  println(howMuch1)
  // ==
  val howMuch2: String =
    if (x < 20) {
      "some"
    } else {
      "many"
    }

  println(howMuch2)
  println()

  /** For - обход коллекций */
  for (a <- 1 to 10) println(s"Value of a: $a")
  println()

  val range: Range.Inclusive = 1 to 3  // 1 to 3 == 1.to(3) - пример инфиксной нотации

  for (x <- range) {
    println(x + 2)
  }

  println()

  /** For-comprehension */
  val res8: Seq[Int] =
    for {
      a <- 1 to 5  // коллекция 1
      b <- 1 to 5  // коллекция 2
      if a + b < 6  // условие
    } yield a + b  // генератор
  // ==
  (1 to 5).flatMap { a =>
    (1 to 5)
      .withFilter(b => a + b < 6)
      .map(b => a + b)
  }

  println(s"for-comprehension: $res8")
  println()


  /** Pattern matching */
  val any: Any = 10

  val res9: Any =
    any match {
      case 1 => "one"
      case "two" => 2
      case y: Int => s"$y is scala.Int"
      case _ => "unknown"
    }

  println(res9)

  def test(val1: Int, val2: Int): (Int, Int) = (val1, val2)

  /** Работает с помощью unapply */
  val (a, b) = test(1, 1)
  println(s"a == $a, b == $b")
  println()


  /** Collections API */
  /** List - эффект множественного значения */
  val fruits1: List[String] = List("apple", "banana", "pear")
  println(fruits1)
  // ==
  val fruits2: List[String] = "apple" :: ("banana" :: ("pear" :: Nil))  // скобки только для удобства
  // :: - правоассоциативный метод => "pear" :: Nil == Nil.::("pear")
  println(fruits2)

  val fill: List[String] = List.fill(10)("apples")
  println(fill)

  val map: List[String] = fruits1.map(el => s"This is $el")
  println(map)

  val contains: Boolean = fruits1.contains("apple")
  println(contains)

  val head: String = fruits1.head  // == fruit(0)
  println(head)

  val take: List[String] = fruits1.take(2)
  println(take)

  val filterEnds: List[String] = fruits1.filter(_.endsWith("e"))
  println(filterEnds)

  val existStarts: Boolean = fruits1.exists(_.startsWith("b"))
  println(existStarts)

  val distinct: List[String] = fruits1.distinct
  println(distinct)

  val size: Int = fruits1.size
  println(size)
  println()

  /** Aggregation */
  val listTup2: List[(String, Int)] = List(("apple", 1), ("apple", 2), ("apple", 3), ("orange", 2))

  val groupBy: Map[String, List[(String, Int)]] = listTup2.groupBy(_._1)
  println(s"groupBy: $groupBy")

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

  println(s"mapValues.mapView: $mapValues")
  println(s"mapValues: ${mapValues.toMap}")
  println()

  val intList2: List[Int] = List(1, 7, 5, 9, 3)

  val reduce: Int = intList2.reduce((el1, el2) => if (el1 > el2) el1 else el2)  // == max
  println(reduce)

  val reduceOption: Option[Int] = List.empty[Int].reduceOption(_ + _)
  println(reduceOption)

  val fold: (Double, Int) = intList2.foldLeft((0.0, 0)) { (acc, el) => (acc._1 + el, acc._2 + 1) }
  println(fold)
  println(fold._1 / fold._2)  // mean
  println()

  /** map: A => B */
  val map2: List[Int] = intList.map(_ + 1)
  println(map2)

  /**
   * flatMap: A => F[B]
   * flatMap == map + flatten
   */
  val flatMap: List[Int] = intList.flatMap(el => List(el, el, el))
  println(flatMap)
  println()

  /** Foreach: A => Unit */
  intList.foreach(println)

  /** flatten G[F[A]] => G[A] - распаковывает внутреннюю коллекцию (монаду) */
  val flatten: List[Int] = List(List(1), List(2, 3), List(4, 5), Nil).flatten
  println(flatten)

  val mapFlatten: List[Int] = intList.map(el => List(el, el, el)).flatten  // == intList.flatMap(el => List(el, el, el))
  println(mapFlatten)
  println()


  /** Map */
  val fruits: Map[String, Int] = Map("apple" -> 2, "banana" -> 1, "pear" -> 10)
  println(fruits)

//  val unsafeValue: Int = fruits("peach")  // err - key not found
  val safeValue1: Int = fruits.getOrElse("peach", 10)
  println(safeValue1)

  val safeValue2: Option[Int] = fruits.get("peach")
  println(safeValue2)
  println()

  val map2List: List[(String, Int)] = fruits.toList
  println(map2List)

  val list2Map: Map[String, Int] = List("apple" -> 2, "banana" -> 1, "pear" -> 10).toMap  // "apple" -> 2 == ("apple", 2)
  println(list2Map)
  println()

  val colors: Map[String, String] = Map("red" -> "#FF0000", "azure" -> "#F0FFFF")
  println(s"Keys in colors: ${colors.keys}")
  println(s"Values in colors: ${colors.values}")
  println(s"Check if colors is empty: ${colors.isEmpty}")
  println()

  val colorsImmutable: Map[String, String] = colors.updated("black", "#000000")  // Immutable updateD result type == Map
  println(colorsImmutable)

  val colorsMutable: mutable.Map[String, String] = mutable.Map("red" -> "#FF0000", "azure" -> "#F0FFFF")
  colorsMutable.update("black", "#000000")  // Mutable update_ result type == ()
  println(colorsMutable)
  println()


  /** Set */
  val set: Set[String] = Set("apple", "banana", "banana", "banana", "pear")
  println(set)
  println()


  /** Tuple - его элементы могут иметь разные типы */
  val t3: (Int, String, Double) = (1, "hello", 3.0)
  println(t3)

  val t4: (Int, Int, Int, Int) = (4, 3, 2, 1)
  println(t4)

  val t4Sum: Int = t4._1 + t4._2 + t4._3 + t4._4
  println(t4Sum)

  var tNull: (Int, String, Double) = null
  println(tNull)
//  tNull = (1, 2, 3.0)  // err - type mismatch
  tNull = (1, 2.toString, 3.0)  // ok
  println(tNull)
  println()


  /** Option == Some(value)/None - эффект возможного отсутствия значения - коллекция с 0/1 элементом */
  val optSome: Option[Int] = Some(3)
  println(optSome)

  val optNone: Option[Int] = None  // == Option.empty[Int]
  println(optNone)

  val someList: List[Int] = Some(3).toList
  println(someList)

  val noneList: List[Int] = None.toList
  println(noneList)
  println()

  val aVal: Int = null.asInstanceOf[Int]  // == 0 - в scala numeric классы не могут принимать значение null
  println(s"aVal: $aVal")

  val bVal: java.lang.Integer = null  // в java numeric классы могут принимать значение null, примитивы - не могут
  println(s"bVal: $bVal")

  val cVal: Int = aVal + bVal
  println(s"cVal: $cVal")
  println()

  val d1Val: Int = bVal
  println(s"d1Val: $d1Val")
  val d2Val: java.lang.Integer = bVal
  println(s"d2Val: $d2Val")
  println()

  val nonEmptyOption: Option[Int] = Some(5)
  println(s"nonEmptyOption.getOrElse(10): ${nonEmptyOption.getOrElse(10)}")
  println(nonEmptyOption.map(_ + 1))

  val emptyOption: Option[Int] = None
  println(s"emptyOption.getOrElse(10): ${emptyOption.getOrElse(10)}")
  println(emptyOption.map(_ + 1))
  println()

  val listWithOption: List[Option[Int]] = List(Some(1), None, Some(2))
  println(s"listWithOption: $listWithOption")

  val flattenedListWithOption: List[Int] = listWithOption.flatten
  println(s"flattenedListWithOption: $flattenedListWithOption")
  println()


  /** Try */
  val iHopeItsNumbers: List[String] = List("1", "2", "banana", "4")
  println(iHopeItsNumbers)

  // toInt == Java Integer.parseInt()
  // try - v1
  def toOptInt(in: String): Option[Int] = Try(in.trim.toInt).toOption

  val res10: List[Option[Int]] = iHopeItsNumbers.map(toOptInt)
  println(res10)

  val res11: List[Int] = iHopeItsNumbers.flatMap(toOptInt)
  println(res11)

  val res12: Int = res11.sum
  println(res12)
  println()


//  val err: Int = "a".toInt  // err - NumberFormatException

  // try - v2
  val `try1` = try {
//    "a".toInt
    5 / 0
  } catch {
    case _: NumberFormatException => println("Bad input string.")
    case e: Throwable => println(e.toString)  // перехват всех ошибок
  }

  println(`try1`)
  println()

  val `try2`: Try[Int] = Try("a".toInt)
  println(`try2`)
  println()

  // v1
  val tryRes1: Int = `try2` match {
    case Failure(_) => 0
    case Success(value) => value
  }

  println(tryRes1)

  // v2
  val tryRes2: Int = `try2`.getOrElse(0)
  println(tryRes2)
  println()


  /** Class */
  class Point(xc: Int, yc: Int) {  // конструктор
    /** Поля( == атрибуты) */
    var x: Int = xc
    var y: Int = yc

    /** Методы */
    def move(dx: Int, dy: Int): Unit = {
      x = x + dx
      y = y + dy

      println(s"Point x location: $x")
      println(s"Point y location: $y")
    }
  }

  val pt: Point = new Point(10, 20)
  pt.move(10, 10)
  println()

  class LinkedList() {  // конструктор 1
    var head: java.lang.Integer = null
    var tail: List[Int] = null

    def isEmpty: Boolean = tail != null

    def this(head: Int) = { this(); this.head = head }  // конструктор 2
    def this(head: Int, tail: List[Int]) = { this(head); this.tail = tail }  // конструктор 3
  }


  /** Case class */
  case class Person(name: String, age: Int)

  val garry: Person = Person("Garry", 22)
  println(garry)

  val goodOldGarry: Person = garry.copy(age = 60)
  println(goodOldGarry)
  println()

  val alice: Person = Person("Alice", 25)
  val bob: Person = Person("Bob", 32)
  val charlie: Person = Person("Charlie", 32)

  for (person <- List(alice, bob, charlie)) {
    person match {
      case Person("Alice", _) => println("Hi Alice!")
      case Person("Bob", 32) => println("Hi Bob!")
      case Person(name, age) => println(s"Age: $age years, name: $name.")
    }
  }

  println()


  /** Object */
  object ColorConfig {
    val options: List[String] = List("red", "green", "blue")
  }

  println(ColorConfig.options)
  println()


  /** Companion object. */
  class MyString(s: String) {
    private var extraData: String = ""
    override def toString: String = s"$s$extraData"
  }

  object MyString {
    def apply(base: String, extras: String): MyString = {  // фабричный метод 1
      val s: MyString = new MyString(base)
      s.extraData = extras

      s
    }

    def apply(base: String): MyString = new MyString(base)  // фабричный метод 2
  }

  println(MyString("hello", " world"))
  println(MyString("hello"))
  println()


  /** Trait */
  // trait Seq[+A] extends Iterable[A] with collection.Seq[A] with SeqOps[A, Seq, Seq[A]] with IterableFactoryDefaults[A, Seq]

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
  abstract class Animal {
    def name: String
  }

  case class Cat(name: String) extends Animal
  case class Dog(name: String) extends Animal


  /** Наследование */
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
  println(d.message)
  println(d.loudMessage)
  println()

  class Complex(real: Double, imaginary: Double) {
    def re: Double = real
    def im: Double = imaginary

    override def toString: String = s"$re ${im}i"
  }



  /** Generic class */
  class Stack[A] {
    private var elements: List[A] = Nil

    def push(x: A): Unit = elements = x :: elements

    def pick: A = elements.head

    def pop(): A = {
      val currentTop: A = pick
      elements = elements.tail
      currentTop
    }
  }

  val intStack: Stack[Int] = new Stack[Int]
  intStack.push(1)
  intStack.push(2)
  println(intStack.pop())
  println(intStack.pop())
  println()

  case class Fruit(name: String)

  val fruitStack: Stack[Fruit] = new Stack[Fruit]
  val apple: Fruit = Fruit("apple")
  val banana: Fruit = Fruit("banana")

  fruitStack.push(apple)
  fruitStack.push(banana)
  println(fruitStack.pop())
  println(fruitStack.pop())
  println()


  /** Модификаторы доступа */
  class AA {
    def publicMethod(): Unit = println("public")
    private def privateMethod(): Unit = println("private")
  }

  abstract class Fruit2 {
    val name: String

    def printName(): Unit = println(s"It's $name")
  }

  /** Анонимный класс - можно реализовать или расширить класс а лету */
  val apple2: Fruit2 = new Fruit2 {
    override val name: String = "apple"
  }

  apple2.printName()
  println()


  /** Implicit class */
  object Helper {
    implicit class StringExtended(str: String) {
      def sayItLoud(): Unit = println(s"${str.toUpperCase}!")
    }
  }

  import Helper.StringExtended
  "hi".sayItLoud()  // == StringExtender("hi").sayItLoud()
  println()


  /** Implicit conversion */
  val flag: Boolean = false
//  val sum1: String = flag + 1  // err - type mismatch

  implicit def bool2Int(b: Boolean): Int = if (b) 1 else 0
  val sum2: Int = flag + 1  // == bool2Int(flag) + 1
  println(sum2)
  println()


  /** Implicit parameter */
  implicit val rub2UsdRate: Double = 75
  def usd2Rub(quantity: Double)(implicit rub2UsdRate: Double): Double = quantity * rub2UsdRate

  println(usd2Rub(10.0))
  println()

  /** Misc */
  val conv1: Long = 2.asInstanceOf[Long]  // bad
  val conv2: Long = 2.toLong  // good

  var counter: Int = 0
  while (counter < 5) {
    println(counter)
    counter += 1
  }
}
