package l_9

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, lit, shuffle, split}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}

object Streaming_8 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_9")
    .getOrCreate()

  import spark.implicits._

  def createConsoleSink(chkName: String, df: DataFrame, queryName: String = "std"): DataStreamWriter[Row] =
    df
      .writeStream
      .queryName(queryName)
      .format("console")
      /** Триггер определяется на уровне sink. */
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_9/chk/$chkName")
      .option("truncate", "false")
      .option("numRows", "20")

  def killAllStreams(): Unit =
    SparkSession
      .active
      .streams
      .active
      .foreach { stream =>
        val description: String = stream.lastProgress.sources.head.description

        stream.stop
        println(s"Stopped $description")
      }

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
      .distinct
      .as[String]
      .collect()

    val columnArray: Array[Column] = idents.map(lit)
    val sparkArray: Column = array(columnArray: _*)
    val shuffledArray: Column = shuffle(sparkArray)

    shuffledArray(0)
  }

  val myStreamDf: DataFrame = spark
    .readStream
    .format("rate")
    .load()
    .withColumn("ident", getRandomIdent())


//  createConsoleSink("state1", myStreamDf, "std1").start()

  /**
   * !!! Стримы на основе одного датафрейма - синхронизированы не будут.
   * ??? Должна быть ошибка: Cannot start query with id as another query with same id is already active - но ее нет.
   * ??? Похоже, второй стрим не создается.
   */
//  createConsoleSink("state1", myStreamDf, "std2").start()

//  Thread.sleep(3000)
//  createConsoleSink("state2", myStreamDf).start()

  /** foreachBatch. */
  def createSink(chkName: String, df: DataFrame)(batchFunc: (DataFrame, Long) => Unit): DataStreamWriter[Row] =
    df
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_9/chk/$chkName")
      .foreachBatch(batchFunc)

  createSink("state3", myStreamDf) { (df, id) =>
    println(s"This is BatchId: $id")
    df.show(1, truncate = false)
  }
//    .start()

  /** !!! Стримовый датафрейм кэшировать нельзя. В foreachBatch - можно. */
  createSink("state4", myStreamDf) { (df, id) =>
    df.cache()
    df.count()

    println(s"This is BatchId: $id")
    df.show(1, truncate = false)
    df.unpersist()
  }
//    .start()

  /** Запись каждого микробатча в паркет. */
  createSink("state5", myStreamDf) { (df, id) =>
    df.cache()
    val count: Long = df.count()
    val schema = df.schema

    println(schema.simpleString)
    println(s"Count: $count")
    println(s"BatchId: $id")
    println()

    df
      .write
      .mode(SaveMode.Append)
      .parquet("src/main/resources/l_9/state5.parquet")

    df.unpersist()
  }
//    .start()

//  val parquetDf: DataFrame = spark
//    .read
//    .parquet("src/main/resources/l_9/state5.parquet")
//
//  println(parquetDf.count())
//  parquetDf.show(20, truncate = false)

  /** Запись в паркет в зависимости от 3-го символа ident. */
  case class Category(name: String, count: Long)

  createSink("state6", myStreamDf) { (df, id) =>
    /** ??? нужен ли здесь df.cache(). */
    df.cache()
    val count: Long = df.count()
    val schema: StructType = df.schema

    println(schema.simpleString)
    println(s"Count:$count")
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

    categories.foreach { category =>
      val cName: String = category.name
      val cCount: Long = category.count

      val filteredDf: Dataset[Row] = withSymbolDf.filter('name === cName)

      filteredDf
        .write
        .mode(SaveMode.Append)
        .parquet(s"src/main/resources/l_9/state6.parquet/$cName")
    }

    withSymbolDf.unpersist()
    df.unpersist()
  }
    .start()

  val parquetDf2: DataFrame = spark
    .read
    .parquet("src/main/resources/l_9/state6.parquet/*")

  println(parquetDf2.count())
  parquetDf2.show(20, truncate = false)

  /** Продолжение с coalesce в Streaming_9. */

  Thread.sleep(1000000)
}
