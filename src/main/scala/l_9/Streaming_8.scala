package l_9

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{array, lit, shuffle, split}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}

object Streaming_8 extends App {
  // не работает в Spark 3.4.0
//  Logger
//    .getLogger("org")
//    .setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_9")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")
  println(sc.uiWebUrl)
  println()

  import spark.implicits._

//  def createConsoleSink(chkName: String, df: DataFrame, queryName: String = "std"): DataStreamWriter[Row] =
  def createConsoleSink(chkName: String, df: DataFrame): DataStreamWriter[Row] =
    df
      .writeStream
//      .queryName(queryName)
      .format("console")
      /** Триггер определяется на уровне sink */
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

        stream.stop()
        println(s"Stopped $description")
      }

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

  val myStreamDf: DataFrame =
    spark
      .readStream
      .format("rate")
      .load()
      .withColumn("ident", getRandomIdent())

//  createConsoleSink("state1", myStreamDf).start()

  /**
   * !!! Стримы на основе одного датафрейма - не могут быть синхронизированы (т.к. не могут использовать общий чекпоинт)
   *
   * err - Cannot start query with name <name> as a query with that name is already active in this SparkSession
   * появляется, если стримы имеют одинаковый queryName
   *
   * Если попробовать запустить два стрима с одинаковым чекпоинтом - второй не запустится
   */
//  createConsoleSink("state1", myStreamDf).start()

//  Thread.sleep(3000)
//  createConsoleSink("state2", myStreamDf).start()

  /** foreachBatch sink */
  def createSink(chkName: String, df: DataFrame)(batchFunc: (DataFrame, Long) => Unit): DataStreamWriter[Row] = {
    df
      .writeStream
//      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_9/chk/$chkName")
      .foreachBatch(batchFunc)
  }

  createSink("state3", myStreamDf) { (df, id) =>
    println(s"BatchId: $id")
    df.show(1, truncate = false)
  }
//    .start()

  /**
   * !!! Стримовый датафрейм кэшировать нельзя
   * В foreachBatch - можно
   */
  createSink("state4", myStreamDf) { (df, id) =>
    df.cache()
    df.count()

    println(s"This is BatchId: $id")
    df.show(1, truncate = false)
    df.unpersist()
  }
//    .start()

  /** Запись каждого микробатча в паркет */
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

//  val parquetDf: DataFrame =
//    spark
//      .read
//      .parquet("src/main/resources/l_9/state5.parquet")
//
//  println(parquetDf.count())
//  parquetDf.show(20, truncate = false)

  /** Запись в паркет в зависимости от 3-го символа ident */
  case class Category(name: String, count: Long)

  createSink("state6", myStreamDf) { (df, id) =>
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
    df.unpersist()

    val categories: Array[Category] =
      withSymbolDf
        .groupBy($"name")
        .count()
        .as[Category]
        .collect()

    categories.foreach { category =>
      val cName: String = category.name
      val cCount: Long = category.count
      println(s"cName: $cName")
      println(s"cCount: $cCount")
      println()

      val filteredDf: Dataset[Row] = withSymbolDf.filter($"name" === cName)

      filteredDf
        .write
        .mode(SaveMode.Append)
        .parquet(s"src/main/resources/l_9/state6.parquet/$cName")
    }

    withSymbolDf.unpersist()
  }
//    .start()

  val parquetDf2: DataFrame =
    spark
      .read
      .parquet("src/main/resources/l_9/state6.parquet/*")

  println(parquetDf2.count())
  parquetDf2.show(20, truncate = false)

  /** Продолжение с coalesce в Streaming_9 */


  Thread.sleep(1000000)

  spark.stop()
}
