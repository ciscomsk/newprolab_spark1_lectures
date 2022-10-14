package l_3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Success, Try}

object RDD extends App {
  // не работает в Spark 3.3.0
//  Logger
//    .getLogger("org")
//    .setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("l_3")
      .master("local[*]")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  val cities: Vector[String] = Vector("Moscow", "Paris", "Madrid", "London", "New York")
  println(cities)
  println(s"The Vector has ${cities.length} elements, the first element is: ${cities.head}")
  println()

  /** RDD[T] */
  val rdd: RDD[String] = sc.parallelize(cities)
  println(s"The RDD has: ${rdd.getNumPartitions} partitions, ${rdd.count()} elements, the first element is: ${rdd.first()}")
  println()

  /** Transformations (например - map) - не запускают вычисления (lazy), выполняется только  построение графа */
  val upperRdd: RDD[String] = rdd.map(_.toUpperCase)

  /** Actions (например - take - передача N первых элементов rdd на драйвер) - запускают выполнение графа */
  val upperVec: Array[String] = upperRdd.take(3)

  // mkString - позволяет сделать из любой локальной коллекции строку
  println(s"Old RDD: ${rdd.take(3).mkString(", ")}")
  println(s"New RDD: ${upperVec.mkString(", ")}")
  println()

  /**
   * reduce - двухэтапная операция:
   * 1. Reduce выполняется на воркерах - результат пересылается на драйвер
   * 2. Reduce выполняется на драйвере - на полученных с воркеров результатах
   */
  val count: Int =
    rdd
      .map(_.length)
      .reduce(_ + _)

  println(s"count: $count")

  val startsWithM: RDD[String] = upperRdd.filter(_.startsWith("M"))
  println(s"The following city names starts with M: ${startsWithM.collect().mkString(", ")}")

  /** count - двухэтапная операция (легковесная) */
  val countM: Long = startsWithM.count()
  println(s"countM: $countM")
  println()


  /** collect - передача всех элементов RDD на драйвер - может привести к OOM */
  val localArray: Array[String] = startsWithM.collect()
  val containsMoscow: Boolean = localArray.contains("MOSCOW")
  println(s"The array contains MOSCOW: $containsMoscow")

  /**
   * take - получение N элементов из RDD
   * Отдает элементы из одной партиции - если в партиции хватает элементов, если нет - будет вычитываться следующие партиции => оптимизация
   */
  val twoFirstElements: Array[String] = startsWithM.take(2)
  println(s"Two first elements of the RDD are: ${twoFirstElements.mkString(", ")}")

  /**
   * takeOrdered - двухэтапная операция - возвращает N минимальных элементов
   * Передает на драйвер N минимальных элементов RDD из каждой партиции (сортировка) - из которых снова выбирается N минимальных элементов (сортировка)
   */
  val twoSortedElements: Array[String] = startsWithM.takeOrdered(2)
  println(s"Two sorted elements of the RDD are: ${twoSortedElements.mkString(", ")}")
  println()


  // "String".toVector == Vector(S, t, r, i, n, g)
  val listStrMap: List[Vector[Char]] = List("String").map(_.toVector)
  println(s"List(\"String\").map(_.toVector): $listStrMap")

  val listStrFlatMap: List[Char] = List("String").flatMap(_.toLowerCase)
  println(s"List(\"String\").flatMap(_.toLowerCase): $listStrFlatMap")
  println()

  val mappedRdd: RDD[Vector[Char]] = rdd.map(_.toVector)
  val strMappedRdd: String = mappedRdd.collect().mkString(", ")
  println(s"rdd.map(_.toVector): $strMappedRdd")

  // String == Collection[Char]
  val flatMappedRdd: RDD[Char] = rdd.flatMap(_.toLowerCase)  // == rdd.map(_.toLowerCase).flatten
  val strFlatMappedRdd: String = flatMappedRdd.collect().mkString(", ")
  println(s"rdd.flatMap(_.toLowerCase): $strFlatMappedRdd")
  println()

  /**
   * distinct - двухэтапная операция
   * 1. distinct выполняется на каждом воркере
   * 2. Запускается шафл(== репартиционирование/перемешивание) - по значению хэша данные собираются на разных воркерах, далее distinct запускается повторно
   */
  val uniqueLetters: RDD[Char] =
    flatMappedRdd
      .distinct()
      .filter(_ != ' ')

  val strUniqueLetters: String = uniqueLetters.collect().sorted.mkString(", ")
  println(s"Unique letters in the RDD are: $strUniqueLetters")
  println()


  val foo: (Int, String) = 3 -> "hello"
  /** get elements - v1 */
  val t1: Int = foo._1
  val t2: String = foo._2
  /** get elements - v2 */
  val (left, right) = foo

  val pairRdd: RDD[(Char, Int)] =
    rdd
      .flatMap(_.toLowerCase)
      .map(x => (x, 1))

  /** countByKey - подсчитывает количество кортежей по каждому ключу и возвращает ЛОКАЛЬНЫЙ Map */
  val letterCount1: collection.Map[Char, Long] = pairRdd.countByKey()
  println(s"letterCount1: $letterCount1")

  /**
   * reduceByKey - работает аналогично обычному reduce, но промежуточный итог накапливается по каждому ключу независимо
   * 1. reduceByKey выполняется в каждой партиции
   * 2. Выполняется репартицирование по ключам
   * 3. reduceByKey выполняется еще раз
   *
   * reduceByKey оптимальнее groupByKey за счет предварительной агрегации в каждой партиции
   */
  val letterCount2: RDD[(Char, Int)] = pairRdd.reduceByKey(_ + _)
  val strLetterCount2: String = letterCount2.collect().mkString(", ")
  println(s"strLetterCount2: $strLetterCount2")
  println()


  /** Option - Some(value)/None */
  val list: List[Int] = List(1, 2, 3)
  println(list.head)

//  println(list.tail.tail.tail.head)  // err - NoSuchElementException
  val headOpt: Option[Int] = list.tail.tail.tail.headOption
  println(headOpt)
  println()

  val opt1: Option[Int] = Some(1)
  val opt2: Option[Int] = Some(2)

  val opt3: Option[Int] =
    for {
      v1 <- opt1
      v2 <- opt2
    } yield v1 + v2

  println(opt3)
  println()

  println(None.getOrElse(0))
  println(None.contains(3))
  println(opt2.map(_ + 1))
  println(opt2.contains(2))
  println(opt2.contains(3))
  println()

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
      .map(x => (x, 1))

  // (p,1), ( ,1), (a,2), (i,2), (y,1), (r,3), (s,2), (k,1), (c,1), (d,3), (l,1), (e,1), (m,2), (n,3), (w,2), (o,5)
  // (a,1), (d,1), (o,1)
  // (Int, Option[Int]) - Option[Int] т.к. left join
  val joined: RDD[(Char, (Int, Option[Int]))] = letterCount2.leftOuterJoin(favLetRdd)

  joined
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
   * !!! Не выведет ничего в консоль драйвера (??? при работе на кластере) т.к. функция foreach применяется к данным НА ВОРКЕРАХ
   * Вывод будет в консоли воркеров
   */
  joined
//    .collect()
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
    map

    def f(x: T): T = _
    rdd.map(f)
    =>
    partition0: Iterator[T]
    partition1: Iterator[T]
    partition2: Iterator[T]
    partition3: Iterator[T]
    =>
    На каждой партиции (p0 - p3) будет выполнен цикл:
    while (partition.hasNext) {
      f(partition.next())
    }
   */

  /*
    mapPartition

    def f(x: Iterator[T]): Iterator[T] = _
    rdd.mapPartitions(f)
    =>
    partition0: Iterator[T]
    partition1: Iterator[T]
    partition2: Iterator[T]
    partition3: Iterator[T]
    =>
    Для каждой партиции (p0 - p3) будет выполнена функция:
    f(p)
   */

  val rdd2: RDD[Int] = sc.parallelize(1 to 1000)

  /**
   * v1 - bad
   *
   * Инстанс connection будет создан на драйвере
   * При попытке сериализации для передачи на воркеры - упадет, т.к. сокеты не сериализуются
   * Если бы сериализация сработала - connection бы создавался для каждого элемента партиции
   */
  //  val connection = new ConnectionToDb(...)

  /**
   * v2 - better
   *
   * Будет создан 1 инстанс для каждой партиции
   */
  rdd.mapPartitions { partition =>  // Iterator[A]
    //    val connection = new ConnectionToDb(...)
    partition.map { elem =>  // A
//      connection.write(elem)
    }
  }

  /**
   * v3 - best
   *
   * Transient lazy val pattern
   * Передается описание конструктора класса
   * Инстанс класса будет создан на каждом воркере (1 на воркер, в mapPartitions - 1 на партицию)
   */
  object Foo {
    @transient
    lazy val connection = 1  // new ConnectionToDb(...)
  }


  /**
   * sc.textFile - позволяет прочитать файл на локальной/S3/HDFS файловой системе
   * С помощью данного метода можно читать как обычные файлы, так и директории с файлами, а также архивы
   */
  val rdd3: RDD[String] = sc.textFile("src/main/resources/l_3/airport-codes.csv")

  rdd3.take(3).foreach(println)
  println()

  /** case class - сериализуется "из коробки" - (extends Serializable) */
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

  val firstElem: String = rdd3.first()  // String - можно сериализовать, rdd - нет
  println(s"firstElem: $firstElem")

  /** Убираем шапку и кавычки */
  val noHeader: RDD[String] =
    rdd3
    .filter(_ != firstElem)
    /**
     * !!! Будет падать (??? при работе на кластере) при попытке сериализовать rdd для передачи на другие воркеры
     * При работе в local mode - все будет выполняться ОЧЕНЬ долго
     */
//    .filter(_ != rdd3.first())
    .map(_.replaceAll("\"", ""))  // \" == символ "

  println(s"noHeader.first(): ${noHeader.first()}")
  println(noHeader.count())
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

  /** !!! Поскольку любые трансформации являются ленивыми, отсутствие ошибок НЕ ОЗНАЧАЕТ, что функция отрабатывает корректно */
  val airportRdd: RDD[Airport] = noHeader.map(toAirport)

  /**
   * Проверить корректность применения функции к данным можно с помощью любого action
   *
   * err: MatchError - означает, что размер массива полученного после операции split, меньше количества переменных
   * которые мы указали в операции val Array(...) = airportArr
   * => перепишем на Option
   */
//  airportRdd.count()

  def toAirportOpt(data: String): Option[Airport] = {
    val airportArr: Array[String] =
      data
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
  println()

  println(s"airportOptRdd.getNumPartitions: ${airportOptRdd.getNumPartitions}")
  println(s"airportOptRdd.count(): ${airportOptRdd.count()}")  // == 55113
  println()

  /** flatMap в данном случае отфильтрует None */
  val airportRdd2: RDD[Airport] = noHeader.flatMap(toAirportOpt)
  println(s"airportRdd2.count(): ${airportRdd2.count()}")  // == 54944
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

  /**
   * err: NumberFormatException - не все строки приводятся к числам
   * => перепишем на Try
   */
//  println(airportTypedRdd.count())

  /** Try ловит только non-fatal ошибки (т.е. нельзя поймать OOM/Sigterm и т.п.) */
  val trySuccess: Try[Int] = Try { 1 / 1 }
  val tryFailure: Try[Int] = Try { 1 / 0 }

  println(trySuccess.getOrElse(0))
  println(trySuccess.map(_ + 1))
  trySuccess.foreach(println)
  println(trySuccess.toOption)
  println()

  println(tryFailure.getOrElse(0))
  println(tryFailure.map(_ + 1))
  tryFailure.foreach(println)  // ничего выведено не будет
  println(tryFailure.toOption)
  println()

  val tryValue: Int =
    tryFailure match {
      case Success(v) => v
      case Failure(_) => 0
    }

  println(tryValue)
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
          elevationFt = Try { elevationFt.toInt }.toOption,
          continent = continent,
          isoCountry = isoCountry,
          isoRegion = isoRegion,
          municipality = municipality,
          gpsCode = gpsCode,
          iataCode = iataCode,
          localCode = localCode,
          longitude = Try { longitude.toDouble }.toOption,
          latitude = Try {latitude.toDouble }.toOption
        ))

      case _ => None
    }
  }

  val airportFinal: RDD[AirportSafe] = noHeader.flatMap(toAirportOtpSafe)
  airportFinal.take(3).foreach(println)
  println(s"airportFinal.count(): ${airportFinal.count()}")
  println()

  val pairAirport: RDD[(String, Option[Int])] = airportFinal.map(el => (el.isoCountry, el.elevationFt))
  println(s"pairAirport.first(): ${pairAirport.first()}")

  /** Get values - v1 - getOrElse */
  val fixedElevation1: RDD[(String, Int)] = pairAirport.map(el => (el._1, el._2.getOrElse(Int.MinValue)))

  /** Get values - v2 - pattern matching */
  val fixedElevation2: RDD[(String, Int)] =
    pairAirport.map {  // partially applied function
      case (k, Some(v)) => (k, v)
      case (k, None) => (k, Int.MinValue)
    }

  println(s"fixedElevation1.first(): ${fixedElevation1.first()}")
  println()

  val result: Array[(String, Int)] =
    fixedElevation1
      .reduceByKey(Math.max) // Math.max == (x, y) if (x > y) x else y
      .collect()
      .sortBy(-_._2) // -_._2 == desc

  result.take(10).foreach(println)
  println()


  /** Broadcast */
  val myMap: Map[Int, String] = Map(1 -> "foo", 2 -> "bar")
  val rdd4: RDD[Int] = sc.parallelize(1 to 100)

  /**
   * myMap будет сериализован и передан в каждую партицию => нагрузка на драйвер/сеть
   * Передача будет осуществляться ТОЛЬКО с драйвера
   */
  rdd4
    .map(el => myMap.get(el))
    .count()

  /**
   * Broadcast variable - read only контейнер, копии которого будут находиться на каждом воркере
   * Распространяется peer-to-peer - т.е. нагрузка распределяется между воркерами
   *
   * Если myMap изменится - myMapBroadcast не изменится
   */
  val myMapBroadcast: Broadcast[Map[Int, String]] = sc.broadcast(myMap)

  rdd4
    .map(el => myMapBroadcast.value.get(el))
    .count()


  /** Accumulator */
  var counter: Int = 0

  /** Бессмысленное выражение - мутабельная переменная сериализуется и передается в каждую партицию, инкрементируется и там же остается */
  rdd4.foreach(_ => counter += 1)
  println(s"counter: $counter")

  val counterAccum: LongAccumulator = sc.longAccumulator("myAccum")
  rdd4.foreach(_ => counterAccum.add(1))
  println(s"rdd4.count(): ${rdd4.count()}")

  /** ??? counterAccum не всегда равен rdd4.count() */
  println(s"counterAccum: $counterAccum")
  println(s"counterAccum.value: ${counterAccum.value}")
  println()


  /** SparkUI */
  println(sc.uiWebUrl)

  sc
    .parallelize(0 to 100)
    .distinct()
    .count()

  Thread.sleep(1000000)

  /** Освобождаем ресурсы кластера */
  spark.stop()
}
