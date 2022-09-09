package l_3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Success, Try}

object RDD extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
      .builder
      .appName("l_3")
      .master("local[*]")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  val cities: Vector[String] = Vector("Moscow", "Paris", "Madrid", "London", "New York")
  println(cities)

  val rdd: RDD[String] = sc.parallelize(cities)
  println(s"The RDD has ${rdd.getNumPartitions} partitions,  ${rdd.count} elements, the first one is ${rdd.first}")

  /** Transformation (map) - не запускают вычисления (lazy), выполняется построение графа */
  val upperRdd: RDD[String] = rdd.map(_.toUpperCase)

  /** Action (take - передача N первых элементов rdd на драйвер) - запускает выполнение графа */
  val upperVec: Array[String] = upperRdd.take(3)
  println(upperVec.mkString("Array(", ", ", ")"))

  // mkString - позволяет сделать из любой локальной коллекции строку.
  val str: String = s"upperRdd: ${upperVec.mkString(", ")}"
  println(str)

  /**
   * Reduce - двухэтапная операция:
   * 1. Reduce выполняется на воркерах - результат пересылается на драйвер
   * 2. Reduce выполняется на драйвере - на полученных с воркеров результатах
   */
  val count: Int =
    rdd
      .map(_.length)
      .reduce(_ + _)

  println(count)

  val startsWithM: RDD[String] = upperRdd.filter(_.startsWith("M"))
  println(s"The following city names starts with M: ${startsWithM.collect.mkString(", ")}")

  /** Count - двухэтапная операция (легковесная) */
  val countM: Long = startsWithM.count
  println(countM)
  println()


  /** Collect - передача всех элементов RDD на драйвер */
  val localArray: Array[String] = startsWithM.collect
  val containsMoscow: Boolean = localArray.contains("MOSCOW")
  println(s"The array contains MOSCOW: $containsMoscow")

  /** Если в партиции хватает элементов - отдает элементы из одной партиции, если нет - будет вычитываться еще одна партиция */
  val twoFirstElements: Array[String] = startsWithM.take(2)
  println(s"Two first elements of the RDD are: ${twoFirstElements.mkString(", ")}")

  /**
   * TakeOrdered - двухэтапная операция - возвращает N минимальных элементов.
   * Передает на драйвер N минимальных элементов RDD из каждой партиции - из которых снова выбирается N минимальных элементов.
   */
  val twoSortedElements: Array[String] = startsWithM.takeOrdered(2)
  println(s"Two sorted elements of the RDD are: ${twoSortedElements.mkString(", ")}")
  println()


  // "String".toVector == Vector(S, t, r, i, n, g)
  val listStrMap: List[Vector[Char]] = List("String").map(_.toVector)
  println(listStrMap)

  val listStrFlatMap: List[Char] = List("String").flatMap(_.toLowerCase)
  println(listStrFlatMap)
  println

  val mappedRdd: RDD[Vector[Char]] = rdd.map(_.toVector)
  val strMappedRdd: String = mappedRdd.collect.mkString(", ")
  println(s"Rdd map(_.toVector): $strMappedRdd")

  // RDD[String] == RDD[Collection[Char]]
  val flatMappedRdd: RDD[Char] = rdd.flatMap(_.toLowerCase)  // == rdd.map(_.toLowerCase).flatten
  val strFlatMappedRdd: String = flatMappedRdd.collect.mkString(", ")
  println(s"Rdd flatMap(_.toLowerCase): $strFlatMappedRdd")
  println()

  /**
   * Distinct - двухэтапная операция.
   * 1. Distinct выполняется на каждом воркере.
   * 2. Запускается шафл - по значению хэша данные собираются на разных воркерах, далее Distinct запускается повторно.
   */
  val uniqueLetters: RDD[Char] =
    flatMappedRdd
      .distinct
      .filter(_ != ' ')

  val strUniqueLetters: String = uniqueLetters.collect.sorted.mkString(", ")
  println(s"Unique letters in the RDD are: $strUniqueLetters")
  println()


  val foo: (Int, String) = 3 -> "hello"
  // v1
  val t1: Int = foo._1
  val t2: String = foo._2
  // v2
  val (left, right) = foo

  val pairRdd: RDD[(Char, Int)] =
    rdd
      .flatMap(_.toLowerCase)
      .map(x => (x, 1))

  /** CountByKey - подсчитывает количество кортежей по каждому ключу и возвращает ЛОКАЛЬНЫЙ Map */
  val letterCount1: collection.Map[Char, Long] = pairRdd.countByKey
  println(letterCount1)

  /**
   * ReduceByKey - работает аналогично обычному reduce, но промежуточный итог накапливается по каждому ключу независимо
   * 1. ReduceByKey выполняется в каждой партиции
   * 2. Выполняется репартицирование (шафл) по ключам
   * 3. ReduceByKey выполняется еще раз
   *
   * ReduceByKey оптимальнее GroupByKey за счет применения reduceByKey сначала в каждой партиции.
   */
  val letterCount2: RDD[(Char, Int)] = pairRdd.reduceByKey(_ + _)
  val strLetterCount2: String = letterCount2.collect.mkString(", ")
  println(strLetterCount2)
  println()


  val list: List[Int] = List(1, 2, 3)
  println(list.head)

//  println(list.tail.tail.tail.head)  // err - NoSuchElementException
  println(list.tail.tail.tail.headOption)
  println()

  val opt1: Option[Int] = Option(1)
  val opt2: Option[Int] = Option(2)

  val opt3: Option[Int] =
    for {
      v1 <- opt1
      v2 <- opt2
    } yield v1 + v2

  println(None.getOrElse(0))
  println(opt2.map(_ + 1))
  println(opt2.contains(3))

  val optVal: Int =
    Option(2) match {
      case Some(v) => v
      case None => Int.MinValue
    }

  println()


  val favoriteLetters: Vector[Char] = Vector('a', 'd', 'o')

  val favLetRdd: RDD[(Char, Int)] =
    sc
      .parallelize(favoriteLetters)
      .map(x => (x, 1))

  // (p,1), ( ,1), (a,2), (i,2), (y,1), (r,3), (s,2), (k,1), (c,1), (d,3), (l,1), (e,1), (m,2), (n,3), (w,2), (o,5)
  // leftOuterJoin
  // (a,1), (d,1), (o,2)
  val joined: RDD[(Char, (Int, Option[Int]))] = letterCount2.leftOuterJoin(favLetRdd)

  joined
    .collect
    .foreach { el =>
      val (letter, (leftCount, rightCount)) = el

      rightCount match {
        case Some(_) => println(s"The letter $letter is my favourite and it appears in the RDD $leftCount times")
        case None => println(s"The letter $letter is not my favourite!")
      }
    }

  println()

  /**
   * !!! Не выведет ничего в консоль драйвера (при работе на кластере?) т.к. функция foreach применяется к данным НА ВОРКЕРАХ
   * Вывод будет в консоли воркеров
   */

  joined
    .foreach { el =>
      val (letter, (leftCount, rightCount)) = el

      rightCount match {
        case Some(_) => println(s"The letter $letter is my favourite and it appears in the RDD $leftCount times")
        case None => println(s"The letter $letter is not my favourite!")
      }
    }

  println()


  /**
   * map/foreach
   * mapPartition/foreachPartition
   */

  /*
    Map

    def f(x: T): T = ???
    rdd.map(f)
    =>
    p0: Iterator[T]
    p1: Iterator[T]
    p2: Iterator[T]
    p3: Iterator[T]
    =>
    На каждой партиции (p0 - p3) будет выполнен цикл:
    while (p.hasNext) {
      f(p.next)
    }
   */

  /*
    MapPartition

    def f(x: Iterator[T]): Iterator[T] = ???
    rdd.mapPartitions(f)
    =>
    p0: Iterator[T]
    p1: Iterator[T]
    p2: Iterator[T]
    p3: Iterator[T]
    =>
    Для каждой партиции (p0 - p3) будет выполнена функция:
    f(p)
   */

  val rdd2: RDD[Int] = sc.parallelize(1 to 1000)

  /**
   * v1
   * Инстанс connection будет создан на драйвере
   * При попытке сериализации для передачи на воркеры - упадет, т.к. сокеты не сериализуются
   * Если с сериализацией ок -
   */
  //  val connection = new ConnectionToDb(...)

  /**
   * v2
   * Будет создан 1 инстанс для каждой партиции
   */
  rdd.mapPartitions { partition =>  // Iterator[A]
    //    val connection = new ConnectionToDb(...)
    partition.map { elem =>  // A
//      connection.write(elem)
    }
  }

  /**
   * v3 - Best! transient lazy val pattern.
   * Передается описание конструктора класса
   * Инстанс класса будет создан на каждом воркере (1 на воркер, в mapPartitions - 1 на партицию)
   */
  object Foo {
    @transient
    lazy val connection = ???  // new ConnectionToDb(...)
  }


  /**
   * sc.textFile - позволяет прочитать файл на локальной/S3/HDFS файловой системе
   * С помощью данного метода можно читать как обычные файлы, так и директории с файлами, а также архивы
   */
  val rdd3: RDD[String] = sc.textFile("src/main/resources/l_3/airport-codes.csv")

  rdd3.take(3).foreach(println)
  println()

  // case class - сериализуется из коробки (extends Serializable)
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

  val firstElem: String = rdd3.first
  println(firstElem)

  // Убираем шапку и кавычки
  val noHeader: RDD[String] =
    rdd3
    .filter(_ != firstElem)
    /** !!! Будет падать (при работе на кластере?) при попытке сериализовать rdd (rdd - несериализуем) для передачи на другие воркеры */
//    .filter(_ != rdd3.first)
    .map(_.replaceAll("\"", ""))

  println(noHeader.first)
  println(noHeader.count)
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

  /**
   * !!! Поскольку любые трансформации являются ленивыми, отсутствие ошибок при получении airportRdd НЕ ОЗНАЧАЕТ,
   * что данная функция отрабатывает корректно на датасете
   */
  val airportRdd: RDD[Airport] = noHeader.map(toAirport)

  /**
   * Проверить применимость (наличие ошибок) функции к данным можно с помощью любого action
   *
   * MatchError - означает, что размер массива полученного после операции split, меньше количества переменных
   * которые мы указали в операции val Array(...) = airportArr
   */
//  airportRdd.count  // err - MatchError

  def toAirportOpt(data: String): Option[Airport] = {
    val airportArr: Array[String] = data
      .split(",", -1)
      .map(_.trim)

    airportArr match {
      case Array(ident, aType, name, elevationFt, continent, isoCountry, isoRegion, municipality, gpsCode, iataCode, localCode, longitude, latitude) =>
        Some(Airport(
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
        ))

      case _ => Option.empty[Airport]  // == None
    }
  }

  val airportOptRdd: RDD[Option[Airport]] = noHeader.map(toAirportOpt)
  airportOptRdd.take(3).foreach(println)

//  println(airportOptRdd.getNumPartitions)
//  val startTime: Long = System.currentTimeMillis
  /** !!! Если rdd3.filter(_ != rdd3.first) */
//  println(airportOptRdd.count)  // == 146.3
//  println(airportOptRdd.repartition(8).count)  // == 153.9
  /** Если rdd3.filter(_ != firstElem) */
  println(airportOptRdd.count) // == 0.3
//  val endTime: Long = System.currentTimeMillis
//  println((endTime.toDouble - startTime) / 1000)
  /** count == 55113 */
  println()

  // flatMap в данном случае отфильтрует None
  val airportRdd2: RDD[Airport] = noHeader.flatMap(toAirportOpt)
  println(airportRdd2.count)
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
        Some(AirportTyped(
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
        ))

      case _ => None
    }
  }

  val airportTypedRdd: RDD[AirportTyped] = noHeader.flatMap(toAirportTyped)
//  println(airportTypedRdd.count)  // err - NumberFormatException

  /** Try ловит только non-fatal ошибки */
  val try1: Try[Int] = Try { 1 / 0 }
  val try2: Try[Int] = Try { 1 / 1 }

  println(try1.getOrElse(0))
  println(try2.getOrElse(0))
  println(try1.map(_ + 1))
  println(try2.map(_ + 1))
  try1.foreach(println)
  try2.foreach(println)
  println()

  val pmRes: Int = try1 match {
    case Success(v) => v
    case Failure(_) => 0
  }
  println(pmRes)

  println(try1.toOption)
  println(try2.toOption)
  println()

  case class AirportSafe(
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

  def toAirportOtpSafe(data: String): Option[AirportSafe] = {
    val airportArr: Array[String] = data.split(",", -1)

    airportArr match {
      case Array(ident, aType, name, elevationFt, continent, isoCountry, isoRegion, municipality, gpsCode, iataCode, localCode, longitude, latitude) =>
        Some(AirportSafe(
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

  val airportFinal: RDD[AirportSafe] = noHeader.flatMap(toAirportOtpSafe)
  airportFinal.take(3).foreach(println)
  println(airportFinal.count)
  println()

  val pairAirport: RDD[(String, Option[Int])] = airportFinal.map(el => (el.isoCountry, el.elevationFt))
  println(pairAirport.first)

  // v1
  val fixedElevation1: RDD[(String, Int)] = pairAirport.map(el => (el._1, el._2.getOrElse(Int.MinValue)))

  // v2
  val fixedElevation2: RDD[(String, Int)] = pairAirport.map {
    case (k, Some(v)) => (k, v)
    case (k, None) => (k, Int.MinValue)
  }

  println(fixedElevation1.first)
  println()

  val result: Array[(String, Int)] =
    fixedElevation1
      .reduceByKey(Math.max) // == (x, y) if (x > y) x else y
      .collect
      .sortBy(-_._2)

  result.take(10).foreach(println)
  println()


  /**
   * Broadcast variable - read only snapshot, копии которого будут находиться на каждом воркере
   * Распространяется peer-to-peer - т.е. нагрузка распределяется между воркерами
   */

  val myMap: Map[Int, String] = Map(1 -> "foo", 2 -> "bar")

  val rdd4: RDD[Int] = sc.parallelize(1 to 100)

  /**
   * myMap будет сериализован и передан в каждую партицию - нагрузка на драйвер/сеть
   * Передача будет осуществляться только с драйвера
   */
  rdd4.map(el => myMap.get(el)).count

  val myMapBroadcast: Broadcast[Map[Int, String]] = sc.broadcast(myMap)
  rdd4.map(el => myMapBroadcast.value.get(el)).count


  /** Accumulator */
  var counter: Int = 0
  /** Бессмысленное выражение - мутабельная переменная сериализуется и передается на каждую партицию, инкрементируется и там же остается */
  rdd4.foreach(_ => counter += 1)

  val counterAccum: LongAccumulator = sc.longAccumulator("myAccum")
  rdd4.foreach(_ => counterAccum.add(1))
  println(counterAccum)


  /** SparkUI */
  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  /** Освобождаем ресурсы кластера. */
  spark.stop()
}
