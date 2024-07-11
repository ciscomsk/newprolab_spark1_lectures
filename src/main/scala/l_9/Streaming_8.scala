package l_9

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{array, lit, shuffle, split}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}

object Streaming_8 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_9")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")
  println(sc.uiWebUrl)
  println()

  import spark.implicits._

//  def createConsoleSink(chkName: String, df: DataFrame, queryName: String = "std"): DataStreamWriter[Row] =
  def createConsoleSink(chkName: String, df: DataFrame): DataStreamWriter[Row] =
    df
      .writeStream
//      .queryName(queryName)
      .format("console")
      /** триггер определяется на уровне sink */
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
        println(s"Stopped: $description")
      }

  def airportsDf(): DataFrame = {
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")
  }

  def getRandomIdent: Column = {
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
      .withColumn("ident", getRandomIdent)

//  createConsoleSink("state1", myStreamDf).start()

  /**
   * !!! Стримы на основе одного источника данных - не могут быть синхронизированы (т.к. не могут использовать общий чекпоинт)
   * если попробовать запустить два стрима с одинаковым чекпоинтом - второй не запустится
   *
   * c queryName:
   * err - Cannot start query with name <name> as a query with that name is already active in this SparkSession
   * появляется, если стримы имеют одинаковый queryName
   *
   *
   * без queryName:
   * WARN StreamingQueryManager: Stopping existing streaming query [id=03ac4435-a637-4a5f-8c71-9cdd3367a2de, runId=c465828e-8815-4eb2-8e88-96d8b7cfa872],
   * as a new run is being started
   */
//  Thread.sleep(5000)
//  createConsoleSink("state1", myStreamDf).start()

  /** на разных чекпоинтах - ок */
//  Thread.sleep(5000)
//  createConsoleSink("state2", myStreamDf).start()

  /** ForeachBatch sink */
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
   * в foreachBatch - датафрейм является статическим => можно
   */
  createSink("state4", myStreamDf) { (df, id) =>
    df.cache()
    df.count()

    println(s"BatchId: $id")
    df.show(1, truncate = false)
    df.unpersist()
  }
//    .start()

  /** Запись каждого микробатча в паркет */
  createSink("state5", myStreamDf) { (df, id) =>
    df.cache()
    val count: Long = df.count()
    val schema: StructType = df.schema

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
      val categoryName: String = category.name
      val categoryCount: Long = category.count
      println(s"categoryName: $categoryName")
      println(s"categoryCount: $categoryCount")
      println()

      val filteredDf: Dataset[Row] = withSymbolDf.filter($"name" === categoryName)

      filteredDf
        .write
        .mode(SaveMode.Append)
        .parquet(s"src/main/resources/l_9/state6.parquet/$categoryName")
    }

    withSymbolDf.unpersist()
  }
//    .start()

//  val parquetDf2: DataFrame =
//    spark
//      .read
//      .parquet("src/main/resources/l_9/state6.parquet/*")
//
//  println(parquetDf2.count())
//  parquetDf2.show(20, truncate = false)

  createSink("state6-coalesce", myStreamDf) { (df, id) =>
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

    val coalescedDf: Dataset[Row] = withSymbolDf.coalesce(1)
    coalescedDf.cache()
    coalescedDf.count()
    withSymbolDf.unpersist()

    categories.foreach { category =>
      val categoryName: String = category.name
      val categoryCount: Long = category.count
      println(s"categoryName: $categoryName")
      println(s"categoryCount: $categoryCount")
      println()

      val filteredDf: Dataset[Row] = coalescedDf.filter($"name" === categoryName)

      filteredDf
        .write
        .mode(SaveMode.Append)
        .parquet(s"src/main/resources/l_9/state6-coalesce.parquet/$categoryName")
    }

    coalescedDf.unpersist()
  }
//    .start()


  Thread.sleep(1_000_000)

  spark.stop()
}
