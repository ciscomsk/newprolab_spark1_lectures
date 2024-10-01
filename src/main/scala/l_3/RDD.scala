package l_3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Success, Try}

object RDD extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("l_3")
      .master("local[*]")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  val cities: Vector[String] = Vector("Moscow", "Paris", "Madrid", "London", "New York")
  println(cities)
  println(s"The Vector has ${cities.length} elements, the first element is: ${cities.head}")
  println()

  /** RDD[T] */
  /*
    package org.apache.spark.rdd

    abstract class RDD[T: ClassTag](
      @transient private var _sc: SparkContext,
      @transient private var deps: Seq[Dependency[_]]
    ) extends Serializable with Logging
   */

  val rdd: RDD[String] = sc.parallelize(cities)
  rdd.cache()
  println(s"The RDD has: ${rdd.getNumPartitions} partitions, ${rdd.count()} elements, the first element is: ${rdd.first()}")
  println()

  /**
   * Transformations (map/flatMap/filter/distinct/...) - не запускают вычисления (т.е. являются lazy), выполняется только построение графа
   * Actions (count/reduce/collect/take/takeOrdered/foreach/...) - запускают выполнение графа
   */

  /** map - применение трансформации к каждому элементу коллекции */
  val upperRdd: RDD[String] = rdd.map(_.toUpperCase)

  /** take - передача N первых элементов RDD на драйвер */
  val upperVec: Array[String] = upperRdd.take(3)

  // mkString - позволяет сделать из любой локальной коллекции строку
  println(s"Old RDD: ${rdd.take(3).mkString(", ")}")
  println(s"New RDD: ${upperVec.mkString(", ")}")
  println()

  /**
   * reduce - двухэтапный action:
   * 1. reduce выполняется в каждой партиции - результат пересылается на драйвер
   * 2. reduce выполняется на драйвере - на агрегированных данных c п.1
   */
  val totalLength: Int =
    rdd
      .map(_.length)
      .reduce(_ + _)

  println(s"Total length: $totalLength")
  println()

  /** filter */
  val startsWithM: RDD[String] = rdd.filter(_.startsWith("M"))
  startsWithM.cache()
  startsWithM.count()
  println(s"The following city names starts with M: ${startsWithM.collect().mkString(", ")}")
  println()

  /**
   * count - легковесный двухэтапный action
   * 1. count выполняется в каждой партиции - результат пересылается на драйвер
   * 2. count выполняется на драйвере - на агрегированных данных c п.1
   */
  val countM: Long = startsWithM.count()
  println(s"countM: $countM")
  println()

  /**
   * collect - передача всех элементов RDD на драйвер
   * !!! может привести к OOM
   */
  val localArray: Array[String] = startsWithM.collect()
  val containsMoscow: Boolean = localArray.contains("MOSCOW")
  println(s"The array contains MOSCOW: $containsMoscow")
  println()

  /**
   * take - передача N первых элементов RDD на драйвер
   * элементы берутся из одной партиции - если в ней хватает элементов, если нет - будут вычитываться следующие партиции - оптимизация
   */
  val twoFirstElements: Array[String] = startsWithM.take(2)
  println(s"Two first elements of the RDD are: ${twoFirstElements.mkString(", ")}")
  println()

  /**
   * takeOrdered - двухэтапный action - передача N минимальных элементов RDD на драйвер
   * 1. сортировка + выборка N минимальных элементов в каждой партиции - передача на драйвер
   * 2. сортировка + выборка N минимальных элементов на драйвере
   */
  val twoSortedElements: Array[String] = startsWithM.takeOrdered(2)
  println(s"Two sorted elements of the RDD are: ${twoSortedElements.mkString(", ")}")
  startsWithM.unpersist()
  println()


  /*
    IDEA: View -> Show Implicits Hints

    package scala.collection.immutable

    final class WrappedString(private val self: String)
      extends AbstractSeq[Char]
      with IndexedSeq[Char]
      with IndexedSeqOps[Char, IndexedSeq, WrappedString]
      with Serializable

    String ~= IndexedSeq[Char]
    "String".toVector = Vector(S, t, r, i, n, g)
   */
  val mappedList: List[Vector[Char]] = List("String").map(_.toVector)
  println(s"List(\"String\").map(_.toVector): $mappedList")

  val flatMappedList: List[Char] = List("String").flatMap(_.toLowerCase)
  println(s"List(\"String\").flatMap(_.toLowerCase): $flatMappedList")
  println()

  val mappedRdd: RDD[Vector[Char]] = rdd.map(_.toVector)
  val mappedCollect: Array[Vector[Char]] = mappedRdd.collect()
  println(s"rdd.map(_.toVector): ${mappedCollect.mkString(", ")}")

  val flatMappedRdd: RDD[Char] = rdd.flatMap(_.toLowerCase) // = rdd.map(_.toLowerCase).flatten
  flatMappedRdd.cache()
  flatMappedRdd.count()
  val flatMappedCollect: Array[Char] = flatMappedRdd.collect()
  println(s"rdd.flatMap(_.toLowerCase): ${flatMappedCollect.mkString(", ")}")
  println()

  /**
   * distinct - двухэтапный transformation
   * 1. distinct выполняется в каждой партиции
   * 2. выполняется шафл(= репартиционирование/перемешивание) - данные перемещаются в новые партиции по значению хэша
   * 3. повторное выполнение distinct в каждой партиции
   */
  val uniqueLettersRdd: RDD[Char] =
    flatMappedRdd
      .distinct()
      .filter(_ != ' ')

  val uniqueLetters: Array[Char] = uniqueLettersRdd.collect().sorted
  flatMappedRdd.unpersist()
  println(s"Unique letters in the RDD are: ${uniqueLetters.mkString(", ")}")
  println()


  /** Tuple */
  val foo: (Int, String) = 3 -> "hello"

  // get elements - v1
  val t1: Int = foo._1
  val t2: String = foo._2

  // get elements - v2
  val (left, right) = foo

  /** PairRDD */
  val pairRdd: RDD[(Char, Int)] =
    rdd
      .flatMap(_.toLowerCase)
      .map(el => (el, 1))

  pairRdd.cache()
  pairRdd.count()
  rdd.unpersist()

  println(s"pairRdd.take(4): ${pairRdd.take(4).mkString("Array(", ", ", ")")}")

  /** countByKey - подсчитывает количество элементов для каждого ключа и возвращает ЛОКАЛЬНЫЙ Map */
  val letterCount1: collection.Map[Char, Long] = pairRdd.countByKey()
  println(s"letterCount1: $letterCount1")

  /**
   * reduceByKey - работает аналогично обычному reduce, но промежуточный итог накапливается по каждому ключу независимо
   * 1. reduceByKey выполняется в каждой партиции
   * 2. выполняется репартицирование по ключам
   * 3. повторное выполнение reduceByKey в каждой партиции
   *
   * reduceByKey оптимальнее groupByKey за счет предварительной агрегации в каждой партиции
   */
  val letterCountRdd: RDD[(Char, Int)] = pairRdd.reduceByKey(_ + _)
  letterCountRdd.cache()
  letterCountRdd.count()

  val letterCount2: Array[(Char, Int)] = letterCountRdd.collect()
  println(s"letterCount2: ${letterCount2.mkString(", ")}")
  pairRdd.unpersist()
  println()


  /** Option - Some(value)/None */
  val list: List[Int] = List(1, 2, 3)
  println(s"list.head: ${list.head}")

//  println(list.tail.tail.tail.head) // err - NoSuchElementException
  val headOpt: Option[Int] = list.tail.tail.tail.headOption
  println(s"headOpt: $headOpt")
  println()

  val opt1: Option[Int] = Some(1)

  val opt2: Option[Int] = Some(2)
//  val opt2: Option[Int] = None

  val forOpt: Option[Int] =
    for {
      v1 <- opt1
      v2 <- opt2
    } yield v1 + v2
  // =
  opt1.flatMap { v1 =>
    opt2.map(v2 => v2 + v1)
  }
  println(s"forOpt: $forOpt")
  println()

  // safe get value - v1 - getOrElse
  println(None.getOrElse(0))
  println(None.contains(3))
  println()

  println(s"opt2: $opt2")
  println(opt2.map(_ + 1))
  println(opt2.contains(2))
  println(opt2.contains(3))
  println()

  // safe get value - v2 - pattern matching
  val optValue: Int =
    opt2 match {
      case Some(v) => v
      case None => Int.MinValue
    }
  println(optValue)
  println()


  val favoriteLetters: Vector[Char] = Vector('a', 'd', 'o')

  val favLetRdd: RDD[(Char, Int)] =
    sc
      .parallelize(favoriteLetters)
      .map(el => (el, 1))

  // letterCountRdd = (p,1), ( ,1), (a,2), (i,2), (y,1), (r,3), (s,2), (k,1), (c,1), (d,3), (l,1), (e,1), (m,2), (n,3), (w,2), (o,5)
  // favLetRdd = (a,1), (d,1), (o,1)
  // (Int, Option[Int]) - Option[Int] т.к. left join
  val joinedRdd: RDD[(Char, (Int, Option[Int]))] = letterCountRdd.leftOuterJoin(favLetRdd)
  joinedRdd.cache()
  joinedRdd.count()
  letterCountRdd.unpersist()

  println(joinedRdd.collect().mkString(", "))
  println()

  joinedRdd
    .collect()
    .foreach { el =>
      val (letter, (leftCount, rightCount)) = el

      rightCount match {
        case Some(_) => println(s"The letter $letter is my favourite and it appears in the RDD $leftCount times")
        case None => println(s"The letter $letter is not my favourite!")
      }
    }
  println()

  /**
   * !!! вывода в консоль драйвера - не будет (при работе на кластере) т.к. foreach выполняется НА ЭКЗЕКЬЮТОРАХ
   * вывод будет в консолях экзекьюторов
   */
  joinedRdd
//    .collect()
    .foreach { el =>
      val (letter, (leftCount, rightCount)) = el

      rightCount match {
        case Some(_) => println(s"The letter $letter is my favourite and it appears in the RDD $leftCount times")
        case None => println(s"The letter $letter is not my favourite!")
      }
    }
  joinedRdd.unpersist()
  println()


  /** map/foreach VS mapPartition/foreachPartition */
  val rdd2: RDD[Int] = sc.parallelize(1 to 1000)

  /*
    map

    def func(el: A): B = _
    rdd2.map(func)

    partition0: Iterator[A]
    partition1: Iterator[A]
    partition2: Iterator[A]
    partition3: Iterator[A]

    в каждой партиции будет выполнен цикл:
    while (partition.hasNext) {
      func(partition.next())
    }
   */

  /*
    mapPartition

    def func(part: Iterator[A]): Iterator[B] = _
    rdd2.mapPartitions(func)

    partition0: Iterator[A]
    partition1: Iterator[A]
    partition2: Iterator[A]
    partition3: Iterator[A]

    на каждой партиции будет выполнена функция:
    func(p)
   */

  /**
   * v1 - bad
   *
   * инстанс connection создается на драйвере
   * при попытке сериализации для передачи на экзекьютор - упадет, т.к. сокеты не сериализуются
   * если бы сериализация сработала - connection пересылался бы для каждого элемента партиции
   */
//  val connection = new ConnectionToDb(...)
//  rdd2.foreach(el => connection.write(el))

  /**
   * v2 - better
   *
   * будет создан 1 инстанс connection для каждой партиции
   */
    rdd2
      .foreachPartition { (partition: Iterator[Int]) => // Iterator[A]
//        val connection = new ConnectionToDb(...)
        partition.map { (el: Int) => // Unit
//          connection.write(el)
        }
  }

  /**
   * v3 - best
   *
   * Transient lazy val pattern
   * вместо сериализации и передачи объекта, созданного на драйвера - передается описание конструктора класса
   * инстанс connection будет создан на каждом экзекьюторе (1 на экзкьютор, в mapPartitions - 1 на партицию)
   *
   * далее можно использовать и map/foreach и mapPartitions/foreachPartition
   */
  object Foo {
//    @transient
//    lazy val connection = new ConnectionToDb(...)
  }


  /**
   * sc.textFile - позволяет прочитать файл на локальной ФС/S3/HDFS
   * с помощью метода textFile можно читать файлы/директории с файлами/архивы
   */
  val rdd3: RDD[String] = sc.textFile("src/main/resources/l_3/airport-codes.csv")
  println(s"rdd3.getNumPartitions: ${rdd3.getNumPartitions}")
  rdd3.cache()
  rdd3.count()

  rdd3.take(3).foreach(println)
  println()

  /** case class - сериализуется "из коробки" (extends Serializable) */
  case class Airport(
                      ident: String,
                      `type`: String,
                      name: String,
                      elevationFt: String,
                      continent: String,
                      isoCountry: String,
                      isoRegion: String,
                      municipality: String,
                      gpsCode: String,
                      iataCode: String,
                      localCode: String,
                      longitude: String,
                      latitude: String
                    )

  val firstElem: String = rdd3.first() // String - сериализуется (implements java.io.Serializable), rdd - тоже (extends Serializable)
  println(s"firstElem: $firstElem")

  /** убираем шапку и кавычки */
  val noHeaderRdd: RDD[String] =
    rdd3
      .filter(_ != firstElem)
      /**
       * !!! будет падать (при работе на кластере)
       * https://issues.apache.org/jira/browse/SPARK-5063 - Spark does not support nested RDDs or performing Spark actions inside of transformations
       * при работе в local mode - будет выполняться ОЧЕНЬ долго
       */
//      .filter(_ != rdd3.first())
      .map(_.replaceAll("\"", "")) // \" = символ "

  noHeaderRdd.cache()
  noHeaderRdd.count()
  rdd3.unpersist()

  println(s"noHeader.first(): ${noHeaderRdd.first()}")
  println(noHeaderRdd.count())
  println()

  def toAirport(data: String): Airport = {
    val airportArr: Array[String] =
      data
        .split(",")
        .map(_.trim)

    val Array(ident, aType, name, elevationFt, continent, isoCountry, isoRegion, municipality, gpsCode, iataCode, localCode, longitude, latitude) = airportArr

    Airport(
      ident = ident,
      `type` = aType,
      name = name,
      elevationFt = elevationFt,
      continent = continent,
      isoCountry = isoCountry,
      isoRegion = isoRegion,
      municipality = municipality,
      gpsCode = gpsCode,
      iataCode = iataCode,
      localCode = localCode,
      longitude = longitude,
      latitude = latitude
    )
  }

  /** !!! поскольку любые трансформации являются ленивыми, отсутствие ошибок НЕ ОЗНАЧАЕТ, что трансформация работает корректно */
  val airportRdd: RDD[Airport] = noHeaderRdd.map(toAirport)

  /**
   * проверить корректность применения трансформации к данным можно с помощью любого action
   *
   * err: scala.MatchError - означает, что размер массива полученного после операции split != количеству переменных,
   * указанных в распаковке val Array(...) = airportArr
   * => используем Option
   */
//  airportRdd.count()

  def toAirportOpt(data: String): Option[Airport] = {
    val airportArr: Array[String] =
      data
        .split(",", -1)
        .map(_.trim)

    airportArr match {
      case Array(ident, aType, name, elevationFt, continent, isoCountry, isoRegion, municipality, gpsCode, iataCode, localCode, longitude, latitude) =>
        Some(
          Airport(
            ident = ident,
            `type` = aType,
            name = name,
            elevationFt = elevationFt,
            continent = continent,
            isoCountry = isoCountry,
            isoRegion = isoRegion,
            municipality = municipality,
            gpsCode = gpsCode,
            iataCode = iataCode,
            localCode = localCode,
            longitude = longitude,
            latitude = latitude
          )
        )

      case _ => Option.empty[Airport] // = None
    }
  }

  val airportOptRdd: RDD[Option[Airport]] = noHeaderRdd.map(toAirportOpt)
  airportOptRdd.cache()
  airportOptRdd.count()

  airportOptRdd.take(3).foreach(println)
  println()

  println(s"airportOptRdd.getNumPartitions: ${airportOptRdd.getNumPartitions}")
  println(s"airportOptRdd.count(): ${airportOptRdd.count()}") // = 55113
  airportOptRdd.unpersist()
  println()

  /** flatMap в данном случае отфильтрует None */
  val airportRdd2: RDD[Airport] = noHeaderRdd.flatMap(toAirportOpt)
  println(s"airportRdd2.count(): ${airportRdd2.count()}") // = 54944
  println()

  case class AirportTyped(
                           ident: String,
                           `type`: String,
                           name: String,
                           elevationFt: Int,
                           continent: String,
                           isoCountry: String,
                           isoRegion: String,
                           municipality: String,
                           gpsCode: String,
                           iataCode : String,
                           localCode: String,
                           longitude: Double,
                           latitude: Double,
                         )

  def toAirportTyped(data: String): Option[AirportTyped] = {
    val airportArr: Array[String] = data.split(",", -1)

    airportArr match {
      case Array (ident, aType, name, evelationFt, continent, isoCountry, isoRegion, municipality, gpsCode, iataCode, localCode, longitude, latitude) =>
        Some(
          AirportTyped(
            ident = ident,
            `type` = aType,
            name = name,
            elevationFt = evelationFt.toInt,
            continent = continent,
            isoCountry = isoCountry,
            isoRegion = isoRegion,
            municipality = municipality,
            gpsCode = gpsCode,
            iataCode = iataCode,
            localCode = localCode,
            longitude = longitude.toDouble,
            latitude = latitude.toDouble
          )
        )

      case _ => None
    }
  }

  val airportTypedRdd: RDD[AirportTyped] = noHeaderRdd.flatMap(toAirportTyped)

  /**
   * err: java.lang.NumberFormatException - не все строки приводятся к числам
   * => используем Try
   */
//  println(airportTypedRdd.count())

  /** Try ловит только non-fatal ошибки - т.е. нельзя поймать OOM/Sigterm и т.п. */
  val trySuccess: Try[Int] = Try { 1 / 1 }
  val tryFailure: Try[Int] = Try { 1 / 0 }

  println(trySuccess.getOrElse(0))
  println(trySuccess.map(_ + 1))
  trySuccess.foreach(println)
  println(trySuccess.toOption)
  println()

  println(tryFailure.getOrElse(0))
  println(tryFailure.map(_ + 1))
  tryFailure.foreach(println) // ничего выведено не будет
  println(tryFailure.toOption)
  println()

  val tryValue: Int =
    tryFailure match {
      case Success(v) => v
      case Failure(_) => 0
    }
  println(s"tryValue: $tryValue")
  println()

  case class AirportTypedSafe(
                          ident: String,
                          `type`: String,
                          name: String,
                          elevationFt: Option[Int],
                          continent: String,
                          isoCountry: String,
                          isoRegion: String,
                          municipality: String,
                          gpsCode: String,
                          iataCode: String,
                          localCode: String,
                          longitude: Option[Double],
                          latitude: Option[Double]
                        )

  def toAirportTypedSafe(data: String): Option[AirportTypedSafe] = {
    val airportArr: Array[String] = data.split(",", -1)

    airportArr match {
      case Array(ident, aType, name, elevationFt, continent, isoCountry, isoRegion, municipality, gpsCode, iataCode, localCode, longitude, latitude) =>
        Some(AirportTypedSafe(
          ident = ident,
          `type` = aType,
          name = name,
          elevationFt = Try(elevationFt.toInt).toOption,
          continent = continent,
          isoCountry = isoCountry,
          isoRegion = isoRegion,
          municipality = municipality,
          gpsCode = gpsCode,
          iataCode = iataCode,
          localCode = localCode,
          longitude = Try(longitude.toDouble).toOption,
          latitude = Try(latitude.toDouble).toOption
        ))

      case _ => None
    }
  }

  val airportFinalRdd: RDD[AirportTypedSafe] = noHeaderRdd.flatMap(toAirportTypedSafe)
  airportFinalRdd.cache()
  airportFinalRdd.count()

  airportFinalRdd.take(3).foreach(println)
  println(s"airportFinal.count(): ${airportFinalRdd.count()}")
  noHeaderRdd.unpersist()
  println()

  val pairAirportRdd: RDD[(String, Option[Int])] = airportFinalRdd.map(el => (el.isoCountry, el.elevationFt))
  pairAirportRdd.cache()
  pairAirportRdd.count()
  airportFinalRdd.unpersist()
  println(s"pairAirport.first(): ${pairAirportRdd.first()}") // = (US,Some(11))


  /** нужно перейти от Option[Int] к Int для удобного сравнения */
  /** Option[A] => A - v1 - getOrElse */
  val fixedElevationRdd1: RDD[(String, Int)] =
    pairAirportRdd.map { case (isoCountry, elevationFt) => (isoCountry, elevationFt.getOrElse(Int.MinValue)) }

  fixedElevationRdd1.cache()
  fixedElevationRdd1.count()
  pairAirportRdd.unpersist()

  /** Option[A] => A - v2 - pattern matching */
  val fixedElevationRdd2: RDD[(String, Int)] =
    pairAirportRdd.map { // partially applied function
      case (k, Some(v)) => (k, v)
      case (k, None) => (k, Int.MinValue)
    }

  println(s"fixedElevation1.first(): ${fixedElevationRdd1.first()}") // = (US,11)
  println()

  val result: Array[(String, Int)] =
    fixedElevationRdd1
      .reduceByKey(Math.max) // Math.max = (x, y) => if (x > y) x else y
      .collect()
      .sortBy { case (_, elevationFt) => -elevationFt } // -elevationFt = desc
      // =
//      .sortBy(-_._2) // -_._2 = desc

  fixedElevationRdd1.unpersist()
  result.take(10).foreach(println)
  println()


  /** Broadcast variables */
  val myMap: Map[Int, String] = Map(1 -> "foo", 2 -> "bar")
  val rdd4: RDD[Int] = sc.parallelize(1 to 100)

  /**
   * myMap будет сериализован и передан в КАЖДУЮ ПАРТИЦИЮ => нагрузка на драйвер/сеть
   * передача будет осуществляться ТОЛЬКО с драйвера
   */
  val resMapRdd: RDD[Option[String]] = rdd4.map(el => myMap.get(el))
  println(s"resMapRdd: ${resMapRdd.take(3).mkString("Array(", ", ", ")")}")

  /**
   * Broadcast variable - read only контейнер, копии которого будут находиться на каждом экзекьюторе
   * распространяется peer-to-peer - т.е. нагрузка распределяется между экзекьюторами
   *
   * myMap - существует только на драйвере
   * myMapBroadcast - read only копия myMap - существует на каждом экзекьюторе
   * !!! если myMap изменится - myMapBroadcast не изменится
   */
  val myMapBroadcast: Broadcast[Map[Int, String]] = sc.broadcast(myMap)
  val resBroadcastRdd: RDD[Option[String]] = rdd4.map(el => myMapBroadcast.value.get(el))
  println(s"resBroadcastRdd: ${resBroadcastRdd.take(3).mkString("Array(", ", ", ")")}")
  println()


  /** Accumulator */
  var counter: Int = 0

  /**
   * бессмысленная операция (при работе на кластере)
   * мутабельная переменная counter сериализуется и передается в каждую партицию, инкрементируется и там же остается
   */
  rdd4.foreach(_ => counter += 1)
  println(s"counter: $counter")
  println()

  val counterAcc: LongAccumulator = sc.longAccumulator("myAcc")
  rdd4.foreach(_ => counterAcc.add(1))

  println(s"counterAcc: $counterAcc")
  /**
   * counterAccum.value иногда не равен rdd4.count() (?) - проблема в Idea?
   * в spark-shell - равен 100
   */
  println(s"counterAcc.value: ${counterAcc.value}")
  println(s"rdd4.count(): ${rdd4.count()}")
  println()


  /** SparkUI */
  println(sc.uiWebUrl)

  val uiRes: Long =
    sc
      .parallelize(1 to 100)
      .distinct()
      .count()

  println(s"uiRes: $uiRes")


  Thread.sleep(1_000_000)

  /** Освобождаем ресурсы кластера */
  spark.stop()
}
