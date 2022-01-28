package l_8

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, expr, lit, shuffle, window}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

import scala.concurrent.Future

object Streaming_7 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_8")
    .getOrCreate

  import spark.implicits._

  def airportsDf(): DataFrame = {
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
      .collect

    val columnArray: Array[Column] = idents.map(lit)
    val sparkArray: Column = array(columnArray: _*)
    val shuffledArray: Column = shuffle(sparkArray)

    shuffledArray(0)
  }

  def createConsoleSink(chkName: String, mode: OutputMode, df: DataFrame): DataStreamWriter[Row] =
    df
      .writeStream
      .outputMode(mode)
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_8/chk/$chkName")
      .option("truncate", "false")
      .option("numRows", "20")

  val identStreamDf: DataFrame = spark
    .readStream
    .format("rate")
    .load()
    .withColumn("ident", getRandomIdent())

  /** Stream-Static - Left Outer join - Enrichment. */
  val rightSideLeftOuterDf: DataFrame = airportsDf()

  val resultLeftOuterDf: DataFrame = identStreamDf
    .join(rightSideLeftOuterDf, Seq("ident"), "left")
    .select('ident, 'name, 'elevation_ft, 'iso_country)


//  resultLeftOuterDf.printSchema()
  /*
    root
     |-- ident: string (nullable = true)
     |-- name: string (nullable = true)
     |-- elevation_ft: integer (nullable = true)
     |-- iso_country: string (nullable = true)
   */

//  resultLeftOuterDf.explain(true)
  /*
    == Parsed Logical Plan ==
    'Project ['ident, 'name, 'elevation_ft, 'iso_country]
    +- Project [ident#49, timestamp#0, value#1L, type#70, name#71, elevation_ft#72, continent#73, iso_country#74, iso_region#75, municipality#76, gps_code#77, iata_code#78, local_code#79, coordinates#80]
       +- Join LeftOuter, (ident#49 = ident#69)
          :- Project [timestamp#0, value#1L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-1881421528088388993))[0] AS ident#49]
          :  +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@34907a49, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@4b8c9729, [], [timestamp#0, value#1L]
          +- Relation [ident#69,type#70,name#71,elevation_ft#72,continent#73,iso_country#74,iso_region#75,municipality#76,gps_code#77,iata_code#78,local_code#79,coordinates#80] csv

    == Analyzed Logical Plan ==
    ident: string, name: string, elevation_ft: int, iso_country: string
    Project [ident#49, name#71, elevation_ft#72, iso_country#74]
    +- Project [ident#49, timestamp#0, value#1L, type#70, name#71, elevation_ft#72, continent#73, iso_country#74, iso_region#75, municipality#76, gps_code#77, iata_code#78, local_code#79, coordinates#80]
       +- Join LeftOuter, (ident#49 = ident#69)
          :- Project [timestamp#0, value#1L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-1881421528088388993))[0] AS ident#49]
          :  +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@34907a49, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@4b8c9729, [], [timestamp#0, value#1L]
          +- Relation [ident#69,type#70,name#71,elevation_ft#72,continent#73,iso_country#74,iso_region#75,municipality#76,gps_code#77,iata_code#78,local_code#79,coordinates#80] csv

    == Optimized Logical Plan ==
    Project [ident#49, name#71, elevation_ft#72, iso_country#74]
    +- Join LeftOuter, (ident#49 = ident#69)
       :- Project [shuffle([00A,00AA,00AK,00AL,00AR,00AS,00AZ,00CA,00CL,00CN,00CO,00FA,00FD,00FL,00GA,00GE,00HI,00ID,00IG,00II], Some(5959636650199286340))[0] AS ident#49]
       :  +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@34907a49, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@4b8c9729, [], [timestamp#0, value#1L]
       +- Project [ident#69, name#71, elevation_ft#72, iso_country#74]
          +- Filter isnotnull(ident#69)
             +- Relation [ident#69,type#70,name#71,elevation_ft#72,continent#73,iso_country#74,iso_region#75,municipality#76,gps_code#77,iata_code#78,local_code#79,coordinates#80] csv

    == Physical Plan ==
    *(2) Project [ident#49, name#71, elevation_ft#72, iso_country#74]
    // Виды джоинов аналогичны статических датафреймов.
    +- *(2) BroadcastHashJoin [ident#49], [ident#69], LeftOuter, BuildRight, false
       :- *(2) Project [shuffle([00A,00AA,00AK,00AL,00AR,00AS,00AZ,00CA,00CL,00CN,00CO,00FA,00FD,00FL,00GA,00GE,00HI,00ID,00IG,00II], Some(5959636650199286340))[0] AS ident#49]
       :  +- StreamingRelation rate, [timestamp#0, value#1L]
       +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [id=#123]
          +- *(1) Filter isnotnull(ident#69)
             +- FileScan csv [ident#69,name#71,elevation_ft#72,iso_country#74] Batched: false, DataFilters: [isnotnull(ident#69)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [IsNotNull(ident)], ReadSchema: struct<ident:string,name:string,elevation_ft:int,iso_country:string>
   */

//  createConsoleSink("state12_StreamStatic_LeftJoin", OutputMode.Append, resultLeftOuterDf).start()

  /** Inner join - Whitelist. */
  val rightSideInnerDf: DataFrame =
    Vector("00FA", "00IG", "00FD")
      .toDF()
      .withColumnRenamed("value", "ident")

  val resultInnerDf: DataFrame = identStreamDf.join(rightSideInnerDf, Seq("ident"), "inner")

//  createConsoleSink("state13_StreamStatic_InnerJoin", OutputMode.Append, resultInnerDf).start()

  /** Left Anti join - Blacklist. */
  val rightSideLeftAntiDf: DataFrame = Vector("00FA", "00IG", "00FD")
    .toDF()
    .withColumnRenamed("value", "ident")

  val resultLeftAnti: DataFrame = identStreamDf.join(rightSideLeftAntiDf, Seq("ident"), "left_anti")

//  createConsoleSink("state14_StreamStatic_LeftAntiJoin", OutputMode.Append, resultLeftAnti).start()


  /** Stream-Stream join. */
  /** v1 - Window. */
  val leftSideWindowDf: Dataset[Row] = spark
    .readStream
    .format("rate")
    .load()
    .withColumn("ident", getRandomIdent())
    .withColumn("left", lit("left"))
    /** !!! Ватермарки у соединяемых датафреймов могу отличаться. */
    .withWatermark("timestamp", "2 hours")
    /** !!! Окна у соединяемых датафреймов должны быть одинаковыми. */
    .withColumn("window", window('timestamp, "1 minute")).as("left")

  val rightSideWindowDf: Dataset[Row] = spark
    .readStream
    .format("rate")
    .load()
    .withColumn("ident", getRandomIdent())
    .withColumn("right", lit("right"))
    .withWatermark("timestamp", "3 hours")
    .withColumn("window", window('timestamp, "1 minute")).as("right")

  val joinExprWindow: Column = expr("""left.value = right.value and left.window = right.window""")

  val joinedWindowDf: DataFrame = leftSideWindowDf
    .join(rightSideWindowDf, joinExprWindow, "inner")
    .select($"left.window", $"left.value", $"left", $"right")

//  joinedWindowDf.explain(true)
  /*
    == Parsed Logical Plan ==
    'Project ['left.window, 'left.value, 'left, 'right]
    +- Join Inner, ((value#132L = value#198L) AND (window#190-T7200000ms = window#256-T10800000ms))
       :- SubqueryAlias left
       :  +- Project [timestamp#131-T7200000ms, value#132L, ident#180, left#184, window#191-T7200000ms AS window#190-T7200000ms]
       :     +- Filter isnotnull(timestamp#131-T7200000ms)
       :        +- Project [named_struct(start, precisetimestampconversion(((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) as double) = (cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) THEN (CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) + cast(1 as bigint)) ELSE CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) END + cast(0 as bigint)) - cast(1 as bigint)) * 60000000) + 0), LongType, TimestampType), end, precisetimestampconversion((((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) as double) = (cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) THEN (CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) + cast(1 as bigint)) ELSE CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) END + cast(0 as bigint)) - cast(1 as bigint)) * 60000000) + 0) + 60000000), LongType, TimestampType)) AS window#191-T7200000ms, timestamp#131-T7200000ms, value#132L, ident#180, left#184]
       :           +- EventTimeWatermark timestamp#131: timestamp, 2 hours
       :              +- Project [timestamp#131, value#132L, ident#180, left AS left#184]
       :                 +- Project [timestamp#131, value#132L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(6132035778621648771))[0] AS ident#180]
       :                    +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@6ea0dfaa, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@5852605c, [], [timestamp#131, value#132L]
       +- SubqueryAlias right
          +- Project [timestamp#197-T10800000ms, value#198L, ident#246, right#250, window#257-T10800000ms AS window#256-T10800000ms]
             +- Filter isnotnull(timestamp#197-T10800000ms)
                +- Project [named_struct(start, precisetimestampconversion(((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) as double) = (cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) THEN (CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) + cast(1 as bigint)) ELSE CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) END + cast(0 as bigint)) - cast(1 as bigint)) * 60000000) + 0), LongType, TimestampType), end, precisetimestampconversion((((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) as double) = (cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) THEN (CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) + cast(1 as bigint)) ELSE CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) END + cast(0 as bigint)) - cast(1 as bigint)) * 60000000) + 0) + 60000000), LongType, TimestampType)) AS window#257-T10800000ms, timestamp#197-T10800000ms, value#198L, ident#246, right#250]
                   +- EventTimeWatermark timestamp#197: timestamp, 3 hours
                      +- Project [timestamp#197, value#198L, ident#246, right AS right#250]
                         +- Project [timestamp#197, value#198L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-8778471189682465383))[0] AS ident#246]
                            +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@7d2d5625, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@29a51ff1, [], [timestamp#197, value#198L]

    == Analyzed Logical Plan ==
    window: struct<start:timestamp,end:timestamp>, value: bigint, left: string, right: string
    Project [window#190-T7200000ms, value#132L, left#184, right#250]
    +- Join Inner, ((value#132L = value#198L) AND (window#190-T7200000ms = window#256-T10800000ms))
       :- SubqueryAlias left
       :  +- Project [timestamp#131-T7200000ms, value#132L, ident#180, left#184, window#191-T7200000ms AS window#190-T7200000ms]
       :     +- Filter isnotnull(timestamp#131-T7200000ms)
       :        +- Project [named_struct(start, precisetimestampconversion(((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) as double) = (cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) THEN (CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) + cast(1 as bigint)) ELSE CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) END + cast(0 as bigint)) - cast(1 as bigint)) * 60000000) + 0), LongType, TimestampType), end, precisetimestampconversion((((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) as double) = (cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) THEN (CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) + cast(1 as bigint)) ELSE CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) END + cast(0 as bigint)) - cast(1 as bigint)) * 60000000) + 0) + 60000000), LongType, TimestampType)) AS window#191-T7200000ms, timestamp#131-T7200000ms, value#132L, ident#180, left#184]
       :           +- EventTimeWatermark timestamp#131: timestamp, 2 hours
       :              +- Project [timestamp#131, value#132L, ident#180, left AS left#184]
       :                 +- Project [timestamp#131, value#132L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(6132035778621648771))[0] AS ident#180]
       :                    +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@6ea0dfaa, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@5852605c, [], [timestamp#131, value#132L]
       +- SubqueryAlias right
          +- Project [timestamp#197-T10800000ms, value#198L, ident#246, right#250, window#257-T10800000ms AS window#256-T10800000ms]
             +- Filter isnotnull(timestamp#197-T10800000ms)
                +- Project [named_struct(start, precisetimestampconversion(((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) as double) = (cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) THEN (CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) + cast(1 as bigint)) ELSE CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) END + cast(0 as bigint)) - cast(1 as bigint)) * 60000000) + 0), LongType, TimestampType), end, precisetimestampconversion((((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) as double) = (cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) THEN (CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) + cast(1 as bigint)) ELSE CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / cast(60000000 as double))) END + cast(0 as bigint)) - cast(1 as bigint)) * 60000000) + 0) + 60000000), LongType, TimestampType)) AS window#257-T10800000ms, timestamp#197-T10800000ms, value#198L, ident#246, right#250]
                   +- EventTimeWatermark timestamp#197: timestamp, 3 hours
                      +- Project [timestamp#197, value#198L, ident#246, right AS right#250]
                         +- Project [timestamp#197, value#198L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-8778471189682465383))[0] AS ident#246]
                            +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@7d2d5625, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@29a51ff1, [], [timestamp#197, value#198L]

    == Optimized Logical Plan ==
    Project [window#190-T7200000ms, value#132L, left#184, right#250]
    +- Join Inner, ((value#132L = value#198L) AND (window#190-T7200000ms = window#256-T10800000ms))
       :- Project [value#132L, left#184, named_struct(start, precisetimestampconversion(((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) as double) = (cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) THEN (CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) + 1) ELSE CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) END + 0) - 1) * 60000000) + 0), LongType, TimestampType), end, precisetimestampconversion(((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) as double) = (cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) THEN (CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) + 1) ELSE CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) END + 0) - 1) * 60000000) + 60000000), LongType, TimestampType)) AS window#190-T7200000ms]
       :  +- Filter isnotnull(timestamp#131-T7200000ms)
       :     +- EventTimeWatermark timestamp#131: timestamp, 2 hours
       :        +- Project [timestamp#131, value#132L, left AS left#184]
       :           +- Filter isnotnull(value#132L)
       :              +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@6ea0dfaa, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@5852605c, [], [timestamp#131, value#132L]
       +- Project [value#198L, right#250, named_struct(start, precisetimestampconversion(((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) as double) = (cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) THEN (CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) + 1) ELSE CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) END + 0) - 1) * 60000000) + 0), LongType, TimestampType), end, precisetimestampconversion(((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) as double) = (cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) THEN (CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) + 1) ELSE CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) END + 0) - 1) * 60000000) + 60000000), LongType, TimestampType)) AS window#256-T10800000ms]
          +- Filter isnotnull(timestamp#197-T10800000ms)
             +- EventTimeWatermark timestamp#197: timestamp, 3 hours
                +- Project [timestamp#197, value#198L, right AS right#250]
                   +- Filter isnotnull(value#198L)
                      +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@7d2d5625, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@29a51ff1, [], [timestamp#197, value#198L]

    == Physical Plan ==
    *(5) Project [window#190-T7200000ms, value#132L, left#184, right#250]
    // StreamingSymmetricHashJoin ~HashJoin.
    +- StreamingSymmetricHashJoin [value#132L, window#190-T7200000ms], [value#198L, window#256-T10800000ms], Inner, condition = [ leftOnly = null, rightOnly = null, both = null, full = null ], state info [ checkpoint = <unknown>, runId = 6f64906f-f513-4003-b071-9ed1877f3ccd, opId = 0, ver = 0, numPartitions = 200], 0, state cleanup [ left key predicate: (input[1, struct<start:timestamp,end:timestamp>, false].end <= 0), right key predicate: (input[1, struct<start:timestamp,end:timestamp>, false].end <= 0) ], 2
       :- Exchange hashpartitioning(value#132L, window#190-T7200000ms, 200), ENSURE_REQUIREMENTS, [id=#302]
       :  +- *(2) Project [value#132L, left#184, named_struct(start, precisetimestampconversion(((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) as double) = (cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) THEN (CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) + 1) ELSE CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) END + 0) - 1) * 60000000) + 0), LongType, TimestampType), end, precisetimestampconversion(((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) as double) = (cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) THEN (CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) + 1) ELSE CEIL((cast((precisetimestampconversion(timestamp#131-T7200000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) END + 0) - 1) * 60000000) + 60000000), LongType, TimestampType)) AS window#190-T7200000ms]
       :     +- *(2) Filter isnotnull(timestamp#131-T7200000ms)
       :        +- EventTimeWatermark timestamp#131: timestamp, 2 hours
       :           +- *(1) Project [timestamp#131, value#132L, left AS left#184]
       :              +- *(1) Filter isnotnull(value#132L)
       :                 +- StreamingRelation rate, [timestamp#131, value#132L]
       +- Exchange hashpartitioning(value#198L, window#256-T10800000ms, 200), ENSURE_REQUIREMENTS, [id=#312]
          +- *(4) Project [value#198L, right#250, named_struct(start, precisetimestampconversion(((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) as double) = (cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) THEN (CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) + 1) ELSE CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) END + 0) - 1) * 60000000) + 0), LongType, TimestampType), end, precisetimestampconversion(((((CASE WHEN (cast(CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) as double) = (cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) THEN (CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) + 1) ELSE CEIL((cast((precisetimestampconversion(timestamp#197-T10800000ms, TimestampType, LongType) - 0) as double) / 6.0E7)) END + 0) - 1) * 60000000) + 60000000), LongType, TimestampType)) AS window#256-T10800000ms]
             +- *(4) Filter isnotnull(timestamp#197-T10800000ms)
                +- EventTimeWatermark timestamp#197: timestamp, 3 hours
                   +- *(3) Project [timestamp#197, value#198L, right AS right#250]
                      +- *(3) Filter isnotnull(value#198L)
                         +- StreamingRelation rate, [timestamp#197, value#198L]
   */

//  createConsoleSink("state15_StreamStream_Window", OutputMode.Append, joinedWindowDf).start()

  /** v2 - Timestamp. */
  val leftTimestampDf: Dataset[Row] = spark
    .readStream
    .format("rate")
    .load()
    .withColumn("ident", getRandomIdent())
    .withColumn("left", lit("left"))
    .withWatermark("timestamp", "2 hours").as("left")

  val rightTimestampDf: Dataset[Row] = spark
    .readStream
    .format("rate")
    .load()
    .withColumn("ident", getRandomIdent())
    .withColumn("right", lit("right"))
    /** Ватермарки не влияют на логику джоина, влияют только на фильтрацию старых дынных. */
    .withWatermark("timestamp", "3 hours").as("right")

  /** Нужно задать ограничение на таймстэмп. */
  val joinExprTimestamp: Column = expr("""left.value = right.value and left.timestamp <= right.timestamp + INTERVAL 1 hour""")

  /** Еще один пример. */
  val joinExprTimestamp2: Column = expr(
    """left.value = right.value and left.timestamp <= right.timestamp + INTERVAL 1 hour and
      |left.timestamp >= right.timestamp - INTERVAL 1 hour
      |""".stripMargin)

  val joinedTimestampDf: DataFrame = leftTimestampDf
    .join(rightTimestampDf, joinExprTimestamp, "inner")
    .select($"left.value", $"left.timestamp", $"left", $"right")

//  joinedTimestampDf.explain(true)
  /*
    == Parsed Logical Plan ==
    'Project ['left.value, 'left.timestamp, 'left, 'right]
    +- Join Inner, ((value#288L = value#346L) AND (timestamp#287-T7200000ms <= cast(timestamp#345-T10800000ms + INTERVAL '01' HOUR as timestamp)))
       :- SubqueryAlias left
       :  +- EventTimeWatermark timestamp#287: timestamp, 2 hours
       :     +- Project [timestamp#287, value#288L, ident#336, left AS left#340]
       :        +- Project [timestamp#287, value#288L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(6807498272023896356))[0] AS ident#336]
       :           +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@77fa509a, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@5f92ef40, [], [timestamp#287, value#288L]
       +- SubqueryAlias right
          +- EventTimeWatermark timestamp#345: timestamp, 3 hours
             +- Project [timestamp#345, value#346L, ident#394, right AS right#398]
                +- Project [timestamp#345, value#346L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-3994709764950670897))[0] AS ident#394]
                   +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@2746b1cc, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@6bcf2780, [], [timestamp#345, value#346L]

    == Analyzed Logical Plan ==
    value: bigint, timestamp: timestamp, left: string, right: string
    Project [value#288L, timestamp#287-T7200000ms, left#340, right#398]
    +- Join Inner, ((value#288L = value#346L) AND (timestamp#287-T7200000ms <= cast(timestamp#345-T10800000ms + INTERVAL '01' HOUR as timestamp)))
       :- SubqueryAlias left
       :  +- EventTimeWatermark timestamp#287: timestamp, 2 hours
       :     +- Project [timestamp#287, value#288L, ident#336, left AS left#340]
       :        +- Project [timestamp#287, value#288L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(6807498272023896356))[0] AS ident#336]
       :           +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@77fa509a, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@5f92ef40, [], [timestamp#287, value#288L]
       +- SubqueryAlias right
          +- EventTimeWatermark timestamp#345: timestamp, 3 hours
             +- Project [timestamp#345, value#346L, ident#394, right AS right#398]
                +- Project [timestamp#345, value#346L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-3994709764950670897))[0] AS ident#394]
                   +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@2746b1cc, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@6bcf2780, [], [timestamp#345, value#346L]

    == Optimized Logical Plan ==
    Project [value#288L, timestamp#287-T7200000ms, left#340, right#398]
    +- Join Inner, ((value#288L = value#346L) AND (timestamp#287-T7200000ms <= timestamp#345-T10800000ms + INTERVAL '01' HOUR))
       :- Filter isnotnull(timestamp#287-T7200000ms)
       :  +- EventTimeWatermark timestamp#287: timestamp, 2 hours
       :     +- Project [timestamp#287, value#288L, left AS left#340]
       :        +- Filter isnotnull(value#288L)
       :           +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@77fa509a, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@5f92ef40, [], [timestamp#287, value#288L]
       +- Filter isnotnull(timestamp#345-T10800000ms)
          +- EventTimeWatermark timestamp#345: timestamp, 3 hours
             +- Project [timestamp#345, value#346L, right AS right#398]
                +- Filter isnotnull(value#346L)
                   +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@2746b1cc, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@6bcf2780, [], [timestamp#345, value#346L]

    == Physical Plan ==
    *(5) Project [value#288L, timestamp#287-T7200000ms, left#340, right#398]
    +- StreamingSymmetricHashJoin [value#288L], [value#346L], Inner, condition = [ leftOnly = null, rightOnly = null, both = (timestamp#287-T7200000ms <= timestamp#345-T10800000ms + INTERVAL '01' HOUR), full = (timestamp#287-T7200000ms <= timestamp#345-T10800000ms + INTERVAL '01' HOUR) ], state info [ checkpoint = <unknown>, runId = 2e3a4534-6664-48e3-91a9-ed780eef9c5c, opId = 0, ver = 0, numPartitions = 200], 0, state cleanup [ left = null, right value predicate: (timestamp#345-T10800000ms <= -3600001000) ], 2
       :- Exchange hashpartitioning(value#288L, 200), ENSURE_REQUIREMENTS, [id=#435]
       :  +- *(2) Filter isnotnull(timestamp#287-T7200000ms)
       :     +- EventTimeWatermark timestamp#287: timestamp, 2 hours
       :        +- *(1) Project [timestamp#287, value#288L, left AS left#340]
       :           +- *(1) Filter isnotnull(value#288L)
       :              +- StreamingRelation rate, [timestamp#287, value#288L]
       +- Exchange hashpartitioning(value#346L, 200), ENSURE_REQUIREMENTS, [id=#444]
          +- *(4) Filter isnotnull(timestamp#345-T10800000ms)
             +- EventTimeWatermark timestamp#345: timestamp, 3 hours
                +- *(3) Project [timestamp#345, value#346L, right AS right#398]
                   +- *(3) Filter isnotnull(value#346L)
                      +- StreamingRelation rate, [timestamp#345, value#346L]
   */

//  createConsoleSink("state16_StreamStream_Timestamp", OutputMode.Append, joinedTimestampDf).start()

  Thread.sleep(10000000)
}
