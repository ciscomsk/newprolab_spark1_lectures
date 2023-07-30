package l_3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Success, Try}

object RDD extends App {
  // не работает в Spark 3.4.0
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

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
  val rdd: RDD[String] = sc.parallelize(cities)
  rdd.cache()
  println(s"The RDD has: ${rdd.getNumPartitions} partitions, ${rdd.count()} elements, the first element is: ${rdd.first()}")
  println()

  /**
   * Transformations (map/flatMap/filter/...) - не запускают вычисления (т.е. являются lazy), выполняется только построение графа
   * Actions (count/reduce/collect/take/takeOrdered/distinct...) - запускают выполнение графа
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
   * 2. reduce выполняется на драйвере - на агрегированных данных
   */
  val totalLength: Int =
    rdd
      .map(_.length)
      .reduce(_ + _)

  println(s"totalLength: $totalLength")
  println()

  /** filter */
  val startsWithM: RDD[String] = upperRdd.filter(_.startsWith("M"))
  startsWithM.cache()
  startsWithM.count()
  println(s"The following city names starts with M: ${startsWithM.collect().mkString(", ")}")
  println()

  /**
   * count - легковесный двухэтапный action
   * 1. count выполняется в каждой партиции - результат пересылается на драйвер
   * 2. count выполняется на драйвере - на агрегированных данных
   * */
  val countM: Long = startsWithM.count()
  println(s"countM: $countM")
  println()

  /** collect - передача всех элементов RDD на драйвер - может привести к OOM */
  val localArray: Array[String] = startsWithM.collect()
  val containsMoscow: Boolean = localArray.contains("MOSCOW")
  println(s"The array contains MOSCOW: $containsMoscow")
  println()

  /**
   * take - передача N первых элементов RDD на драйвер
   * элементы берутся из одной партиции - если в ней хватает элементов, если нет - будут вычитываться следующие партиции => оптимизация
   */
  val twoFirstElements: Array[String] = startsWithM.take(2)
  println(s"Two first elements of the RDD are: ${twoFirstElements.mkString(", ")}")
  println()

  /**
   * takeOrdered - двухэтапный action - передача N минимальных элементов RDD на драйвер
   * 1. выборка N минимальных элементов из каждой партиции (сортировка) + передача на драйвер
   * 2. выборка N минимальных элементов (сортировка) на драйвере
   */
  val twoSortedElements: Array[String] = startsWithM.takeOrdered(2)
  println(s"Two sorted elements of the RDD are: ${twoSortedElements.mkString(", ")}")
  startsWithM.unpersist()
  println()


  /*
    String == Seq[Char]
    "String".toVector == Vector(S, t, r, i, n, g)
   */
  val listStrMap: List[Vector[Char]] = List("String").map(_.toVector)
  println(s"List(\"String\").map(_.toVector): $listStrMap")

  val listStrFlatMap: List[Char] = List("String").flatMap(_.toLowerCase)
  println(s"List(\"String\").flatMap(_.toLowerCase): $listStrFlatMap")
  println()

  val mappedRdd: RDD[Vector[Char]] = rdd.map(_.toVector)
  val strMappedRdd: String = mappedRdd.collect().mkString(", ")
  println(s"rdd.map(_.toVector): $strMappedRdd")

  val flatMappedRdd: RDD[Char] = rdd.flatMap(_.toLowerCase)  // == rdd.map(_.toLowerCase).flatten
  val strFlatMappedRdd: String = flatMappedRdd.collect().mkString(", ")
  println(s"rdd.flatMap(_.toLowerCase): $strFlatMappedRdd")
  println()

  /**
   * distinct - двухэтапный action
   * 1. distinct выполняется в каждой партиции
   * 2. выполняется шафл(== репартиционирование/перемешивание) - данные перемещаются в партиции по значению хэша
   * 3. повторное выполнение distinct в каждой партиции
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

  /** PairRDD */
  val pairRdd: RDD[(Char, Int)] =
    rdd
      .flatMap(_.toLowerCase)
      .map(x => (x, 1))

  pairRdd.cache()
  pairRdd.count()
  rdd.unpersist()

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
  val letterCount2Rdd: RDD[(Char, Int)] = pairRdd.reduceByKey(_ + _)
  letterCount2Rdd.cache()
  letterCount2Rdd.count()

  val strLetterCount2: String = letterCount2Rdd.collect().mkString(", ")
  println(s"strLetterCount2: $strLetterCount2")
  pairRdd.unpersist()
  println()


  /** Option - Some(value)/None */
  val list: List[Int] = List(1, 2, 3)
  println(s"list.head: ${list.head}")

//  println(list.tail.tail.tail.head)  // err - NoSuchElementException
  val headOpt: Option[Int] = list.tail.tail.tail.headOption
  println(s"headOpt: $headOpt")
  println()

  val opt1: Option[Int] = Some(1)
//  val opt2: Option[Int] = Some(2)
  val opt2: Option[Int] = None

  val forOpt: Option[Int] =
    for {
      v1 <- opt1
      v2 <- opt2
    } yield v1 + v2
  println(s"forOpt: $forOpt")
  println()

  // safe - v1 - getOrElse
  println(None.getOrElse(0))
  println(None.contains(3))
  println(opt2.map(_ + 1))
  println(opt2.contains(2))
  println(opt2.contains(3))
  println()

  // safe - v2 - pattern matching
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

  // letterCount2 == (p,1), ( ,1), (a,2), (i,2), (y,1), (r,3), (s,2), (k,1), (c,1), (d,3), (l,1), (e,1), (m,2), (n,3), (w,2), (o,5)
  // favLetRdd    == (a,1), (d,1), (o,1)
  // (Int, Option[Int]) - Option[Int] т.к. left join
  val joinedRdd: RDD[(Char, (Int, Option[Int]))] = letterCount2Rdd.leftOuterJoin(favLetRdd)
  joinedRdd.cache()
  joinedRdd.count()
  letterCount2Rdd.unpersist()

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
   * !!! Не выведет ничего в консоль драйвера (при работе на кластере) т.к. функция foreach применяется к данным НА ЭКЗЕКЬЮТОРАХ
   * Вывод будет в консолях экзекьюторов
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

  /**
   * map/foreach
   * VS
   * mapPartition/foreachPartition
   */

  val rdd2: RDD[Int] = sc.parallelize(1 to 1000)

  /*
    map

    def f(x: T): T = _
    rdd2.map(f)
    =>
    partition0: Iterator[T]
    partition1: Iterator[T]
    partition2: Iterator[T]
    partition3: Iterator[T]
    =>
    на каждой партиции будет выполнен цикл:
    while (partition.hasNext) {
      f(partition.next())
    }
   */

  /*
    mapPartition

    def f(x: Iterator[T]): Iterator[T] = _
    rdd2.mapPartitions(f)
    =>
    partition0: Iterator[T]
    partition1: Iterator[T]
    partition2: Iterator[T]
    partition3: Iterator[T]
    =>
    для каждой партиции будет выполнена функция:
    f(p)
   */

  /**
   * v1 - bad
   *
   * Инстанс connection создается на драйвере
   * При попытке сериализации для передачи на экзекьютор - упадет, т.к. сокеты не сериализуются
   * Если бы сериализация сработала - connection бы создавался для каждого элемента партиции
   */
  //  val connection = new ConnectionToDb(...)

  /**
   * v2 - better
   *
   * Будет создан 1 инстанс connection для каждой партиции
   */
  rdd2.mapPartitions { partition =>  // Iterator[A]
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
   * Инстанс класса connection будет создан на каждом экзекьюторе (1 на экзкьютор, в mapPartitions - 1 на партицию)
   */
  object Foo {
    @transient
    lazy val connection = 1  // new ConnectionToDb(...)
  }


  /**
   * sc.textFile - позволяет прочитать файл на локальной/S3/HDFS файловой системе
   * С помощью метода textFile можно читать файлы/директории с файлами/архивы
   */
  val rdd3: RDD[String] = sc.textFile("src/main/resources/l_3/airport-codes.csv")
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

  val firstElem: String = rdd3.first()  // String - сериализуется (implements java.io.Serializable), rdd - тоже (extends Serializable)
  println(s"firstElem: $firstElem")

  /** Убираем шапку и кавычки */
  val noHeaderRdd: RDD[String] =
    rdd3
      .filter(_ != firstElem)
      /**
       * !!! Будет падать (при работе на кластере)
       * https://issues.apache.org/jira/browse/SPARK-5063 - Spark does not support nested RDDs or performing Spark actions inside of transformations
       * При работе в local mode - будет выполняться ОЧЕНЬ долго
       */
//      .filter(_ != rdd3.first())
      .map(_.replaceAll("\"", "")) // \" == символ "

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

  /** !!! Поскольку любые трансформации являются ленивыми, отсутствие ошибок НЕ ОЗНАЧАЕТ, что трансформация отрабатывает корректно */
  val airportRdd: RDD[Airport] = noHeaderRdd.map(toAirport)

  /**
   * Проверить корректность применения трансформации к данным можно с помощью любого action
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

      case _ => Option.empty[Airport]  // == None
    }
  }

  val airportOptRdd: RDD[Option[Airport]] = noHeaderRdd.map(toAirportOpt)
  airportOptRdd.cache()
  airportOptRdd.count()

  airportOptRdd.take(3).foreach(println)
  println()

  println(s"airportOptRdd.getNumPartitions: ${airportOptRdd.getNumPartitions}")
  println(s"airportOptRdd.count(): ${airportOptRdd.count()}")  // == 55113
  airportOptRdd.unpersist()
  println()

  /** flatMap в данном случае отфильтрует None */
  val airportRdd2: RDD[Airport] = noHeaderRdd.flatMap(toAirportOpt)
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

  val airportFinalRdd: RDD[AirportSafe] = noHeaderRdd.flatMap(toAirportOtpSafe)
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
  println(s"pairAirport.first(): ${pairAirportRdd.first()}")


  /** Get values from Option - v1 - getOrElse */
  val fixedElevationRdd1: RDD[(String, Int)] =
    pairAirportRdd.map { case (isoCountry, elevationFt) => (isoCountry, elevationFt.getOrElse(Int.MinValue)) }

  fixedElevationRdd1.cache()
  fixedElevationRdd1.count()
  pairAirportRdd.unpersist()

  /** Get values from Option - v2 - pattern matching */
  val fixedElevationRdd2: RDD[(String, Int)] =
    pairAirportRdd.map {  // partially applied function
      case (k, Some(v)) => (k, v)
      case (k, None) => (k, Int.MinValue)
    }

  println(s"fixedElevation1.first(): ${fixedElevationRdd1.first()}")
  println()

  val result: Array[(String, Int)] =
    fixedElevationRdd1
      .reduceByKey(Math.max)  // Math.max == (x, y) if (x > y) x else y
      .collect()
      .sortBy { case (_, elevationFt) => -elevationFt }  // - == desc
      // ==
//      .sortBy(-_._2)  // -_._2 == desc

  fixedElevationRdd1.unpersist()
  result.take(10).foreach(println)
  println()


  /** Broadcast variables */
  val myMap: Map[Int, String] = Map(1 -> "foo", 2 -> "bar")
  val rdd4: RDD[Int] = sc.parallelize(1 to 100)

  /**
   * myMap будет сериализован и передан в КАЖДУЮ партицию => нагрузка на драйвер/сеть
   * передача будет осуществляться ТОЛЬКО с драйвера
   */
  rdd4
    .map(el => myMap.get(el))
    .count()

  /**
   * Broadcast variable - read only контейнер, копии которого будут находиться на каждом экзекьюторе
   * распространяется peer-to-peer - т.е. нагрузка распределяется между экзекьюторами
   *
   * myMap - существует только на драйвере
   * myMapBroadcast - read only копия myMap, существующая на каждом экзекьюторе
   * если myMap изменится - myMapBroadcast не изменится
   */
  val myMapBroadcast: Broadcast[Map[Int, String]] = sc.broadcast(myMap)

  rdd4
    .map(el => myMapBroadcast.value.get(el))
    .count()


  /** Accumulator */
  var counter: Int = 0

  /**
   * Бессмысленное выражение (при работе на кластере)
   * мутабельная переменная сериализуется и передается в каждую партицию, инкрементируется и там же остается
   */
  rdd4.foreach(_ => counter += 1)
  println(s"counter: $counter")
  println()

  val counterAccum: LongAccumulator = sc.longAccumulator("myAccum")
  rdd4.foreach(_ => counterAccum.add(1))

  println(s"counterAccum: $counterAccum")
  /** counterAccum.value иногда не равен rdd4.count() (?) */
  println(s"counterAccum.value: ${counterAccum.value}")
  println(s"rdd4.count(): ${rdd4.count()}")
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
