package l_9

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, lit, shuffle, split, udf}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.annotation.tailrec
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Try

object Streaming_12 extends App {
  // не работает в Spark 3.4.0
//  Logger
//    .getLogger("org")
//    .setLevel(Level.ERROR)

  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("l_9")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.scheduler.allocation.file", "src/main/resources/fairscheduler.xml")

  val spark: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")
  println(sc.uiWebUrl)
  println(spark.conf.get("spark.scheduler.mode"))
  println()

  import spark.implicits._

  case class Category(name: String, count: Long)

  def airportsDf(): DataFrame = {
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")
  }

  def getRandomIdent(): Column = {
    val idents: Array[String] =
      airportsDf()
        .select($"ident")
        .limit(20)
        .distinct()
        .as[String]
        .collect()

    val columnArray: Array[Column] = idents.map(lit)
    val sparkArray: Column = array(columnArray: _*)
    val shuffledArray: Column = shuffle(sparkArray)

    shuffledArray(0)
  }

  def createSink(chkName: String, df: DataFrame)(batchFunc: (DataFrame, Long) => Unit): DataStreamWriter[Row] = {
    df
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_9/chk/$chkName")
      .foreachBatch(batchFunc)
  }

  val myStreamDf: DataFrame =
    spark
      .readStream
      .format("rate")
      .load()
      .withColumn("ident", getRandomIdent())


  import scala.concurrent.ExecutionContext.Implicits.global

  def waitFunc(n: Int): Boolean = { Thread.sleep(n); true }

  println(waitFunc(1000))  // == true
  println(Option(waitFunc(1000)))  // == Some(true)
  println(Try(waitFunc(1000)))  // == Success(true)
  println(Future(waitFunc(1000)))  // == Future(<not completed>)
  println()

  val future1: Future[Boolean] = Future { waitFunc(1000) }

  future1
    .map(el => !el)
    .foreach(el => println(s"mappedFuture: $el \n"))

  val future2: Future[Boolean] = Future { waitFunc(1000) }
  /** !!! Await.result - блокирует поток и ожидает окончания Future */
  println(s"Await future2: ${Await.result(future2, 20 seconds)}")
  println()

  val future3: Future[Boolean] = Future { waitFunc(2000) }
  val future4: Future[Boolean] = Future { waitFunc(3000) }
  val listFutures: List[Future[Boolean]] = List(future3, future4)

  /** sequence == List[Future[A] => Future[List[A]] */
  val futureList: Future[List[Boolean]] = Future.sequence(listFutures)
  println(s"Await futureList: ${Await.result(futureList, 20 seconds)}")
  println()


  /** Перепишем обработку стрима с par на Future */
  val udf_wait: UserDefinedFunction = udf { () => Thread.sleep(1000); true }

  /** List[Int] => List[String] без использования map */
  def recursiveFunctionExample(list: List[Int]): List[String] = {
    val head: Int = list.head
    val tail: List[Int] = list.tail
    println(s"head: $head, tail: ${tail.mkString(" ")}")

    if (tail.isEmpty) head.toString :: Nil
    else head.toString :: recursiveFunctionExample(tail)
  }

  println(recursiveFunctionExample(List(1, 2, 3, 4, 5)))
  println()

  def recAction_v1(categories: List[Category], df: DataFrame): List[Future[Unit]] = {
    val head: Category = categories.head
    val tail: List[Category] = categories.tail

    val future: Future[Unit] =
      Future {
        df
          .filter($"name" === head.name)
          .withColumn("wait", udf_wait())
          .write
          .mode(SaveMode.Append)
          .parquet(s"src/main/resources/l_9/state9.parquet_rec_v1/${head.name}")
      }

    if (tail.isEmpty) future :: Nil
    else future :: recAction_v1(tail, df)
  }

  def recAction_v2(categories: List[Category], df: DataFrame): List[Future[Unit]] = {
    categories match {
      case head :: tail =>
        val future: Future[Unit] =
          Future {
            df
              .filter($"name" === head.name)
              .withColumn("wait", udf_wait())
              .write
              .mode(SaveMode.Append)
              .parquet(s"src/main/resources/l_9/state9.parquet_rec_v2/${head.name}")
          }
        future :: recAction_v2(tail, df)

      case Nil => Nil
    }
  }

  def tailrecAction(cats: List[Category], df: DataFrame): List[Future[Unit]] = {
    @tailrec
    def loop(categories: List[Category] = cats, actions: List[Future[Unit]] = Nil): List[Future[Unit]] =
      categories match {
        case Nil => actions

        case head :: tail =>
          val thisAction: Future[Unit] =
            Future {
              df
                .filter($"name" === head.name)
                .withColumn("wait", udf_wait())
                .write
                .mode(SaveMode.Append)
                .parquet(s"src/main/resources/l_9/state9.parquet_tailrec/${head.name}")
            }

          loop(tail, thisAction :: actions)
      }

    loop()
  }

//  createSink("state9_rec_v1", myStreamDf) { (df, id) =>
  createSink("state9_rec_v2", myStreamDf) { (df, id) =>
//  createSink("state9_tailrec", myStreamDf) { (df, id) =>
    df.cache()
    val count: Long = df.count()
    val schema: StructType = df.schema

    println(schema.simpleString)
    println(s"Count: $count")
    println(s"BatchId: $id")
    println()

    val withSymbolDf: DataFrame = df.withColumn("name", split($"ident", "")(2))
    withSymbolDf.cache()
    withSymbolDf.count()

    val categories: Array[Category] =
      withSymbolDf
        .groupBy($"name")
        .count()
        .as[Category]
        .collect()

    val coalescedDf: Dataset[Row] = withSymbolDf.coalesce(1)

    /**
     * recAction_v1
     *
     * Т.к. первый батч - пустой => err: NoSuchElementException: head of empty list
     * categories.nonEmpty - проверка нужна только для recAction_v1
     */
//    if (categories.nonEmpty) {
//      val listFutures: List[Future[Unit]] =
//        recAction(categories.toList, coalescedDf) :+ Future { df.show(20, truncate = false) }
//
//      val futureList: Future[List[Unit]] = Future.sequence(listFutures)
//
      /** !!! Блокируем тред - чтобы выполнить все операции над текущим микробатчем до начала обработки следующего */
//      Await.result(futureList, 60 seconds)
//    }

    /** recAction_v2  */
    val listFutures: List[Future[Unit]] =
    Future {df.show(20, truncate = false)} :: recAction_v2(categories.toList, coalescedDf)

    val futureList: Future[List[Unit]] = Future.sequence(listFutures)
    Await.result(futureList, 60 seconds)

    /** tailrecAction */
//    val listFutures: List[Future[Unit]] =
//      Future { df.show(20, truncate = false) } :: tailrecAction(categories.toList, coalescedDf)
//
//    val futureList: Future[List[Unit]] = Future.sequence(listFutures)
//    Await.result(futureList, 60 seconds)

    withSymbolDf.unpersist()
    df.unpersist()
  }
  .start()


  Thread.sleep(1000000)

  spark.stop()
}
