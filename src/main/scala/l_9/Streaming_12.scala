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
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("l_9")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.scheduler.allocation.file", "src/main/resources/fairscheduler.xml")

  val spark: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  println(sc.uiWebUrl)
  println(spark.conf.get("spark.scheduler.mode"))

  case class Category(name: String, count: Long)

  def airportsDf() = {
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")
  }

  def getRandomIdent(): Column = {
    val idents: Array[String] = airportsDf()
      .select('ident)
      .limit(20)
      .distinct()
      .as[String]
      .collect()

    val columnArray: Array[Column] = idents.map(lit)
    val sparkArray: Column = array(columnArray: _*)
    val shuffledArray: Column = shuffle(sparkArray)

    shuffledArray(0)
  }

  def createSink(chkName: String, df: DataFrame)(batchFunc: (DataFrame, Long) => Unit): DataStreamWriter[Row] =
    df
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_9/chk/$chkName")
      .foreachBatch(batchFunc)

  val myStreamDf: DataFrame = spark
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

  val future1: Future[Boolean] = Future { waitFunc(1000) }

  future1
    .map(el => el && true)  // el && true == el
    .foreach(println)

  val future2: Future[Boolean] = Future { waitFunc(1000) }
  /** !!! Await.result - блокирует поток и ожидает окончания вычислений в Future. */
  println(Await.result(future2, 20 seconds))

  val future3: Future[Boolean] = Future { waitFunc(2000) }
  val future4: Future[Boolean] = Future { waitFunc(3000) }

  val tmpList: List[Future[Boolean]] = List(future3, future4)

  /** sequence == List[Future[A] => Future[List[A]] */
  val futureList: Future[List[Boolean]] = Future.sequence(future3 :: future4 :: Nil)
  println(Await.result(futureList, 20 seconds))
  println()


  /** Перепишем обработку стрима с par на future. */

  val udf_wait: UserDefinedFunction = udf { () => Thread.sleep(1000); true }

  def recursiveFunctionExample(list: List[Int]): List[String] = {
    val head: Int = list.head
    val tail: List[Int] = list.tail
    println(s"head: $head, tail: ${tail.mkString(" ")}")

    if (tail.nonEmpty) head.toString +: recursiveFunctionExample(tail)
    else head.toString :: Nil
  }

  def recAction(categories: List[Category], df: DataFrame): List[Future[Unit]] = {
    val head: Category = categories.head
    val tail: List[Category] = categories.tail

    val future: Future[Unit] = Future {
      df
        .filter('name === head.name)
        .withColumn("wait", udf_wait())
        .write
        .mode(SaveMode.Append)
        .parquet(s"src/main/resources/l_9/state9/${head.name}")
    }

    if (tail.isEmpty) future :: Nil
    else future +: recAction(tail, df)
  }

  def recActionTailrec(cats: List[Category], df: DataFrame): List[Future[Unit]] = {
    @tailrec
    def loop(categories: List[Category] = cats, actions: List[Future[Unit]] = Nil): List[Future[Unit]] =
      categories match {
        case Nil => actions

        case head :: tail =>
          val thisAction: Future[Unit] = Future {
            df
              .filter('name === head.name)
              .withColumn("wait", udf_wait())
              .write
              .mode(SaveMode.Append)
              .parquet(s"src/main/resources/l_9/state9/${head.name}")
          }

          loop(tail, thisAction :: actions)
      }

    loop()
  }

  createSink("state9.parquet", myStreamDf) { (df, id) =>
    df.cache()
    val count: Long = df.count()
    val schema: StructType = df.schema

    println(schema.simpleString)
    println(s"Count: $count")
    println(s"BatchId: $id")
    println()

    val withSymbolDf: DataFrame = df.withColumn("name", split('ident, "")(2))

    withSymbolDf.cache()
    withSymbolDf.count()

    val categories: Array[Category] = withSymbolDf
      .groupBy('name)
      .count()
      .as[Category]
      .collect()

    val coalescedDf: Dataset[Row] = withSymbolDf.coalesce(1)

    /** Т.к. первый батч - пустой => err: NoSuchElementException: head of empty list */
    if (categories.nonEmpty) {
      val listFutures: List[Future[Unit]] = recActionTailrec(categories.toList, coalescedDf) :+ Future { df.show(20, truncate = false) }
      val futureList: Future[List[Unit]] = Future.sequence(listFutures)

      /** Блокируем тред - чтобы выполнить все операции над текущим микробатчем до начала обработки следующего. */
      Await.result(futureList, 60 seconds)
    }

    withSymbolDf.unpersist()
    df.unpersist()
  }
  .start()

  Thread.sleep(10000000)
}
