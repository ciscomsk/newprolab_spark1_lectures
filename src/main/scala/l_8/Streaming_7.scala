package l_8

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{array, expr, lit, shuffle, window}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

import scala.concurrent.Future

object Streaming_7 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_8")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")
  println(sc.uiWebUrl)
  println()

  import spark.implicits._

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

  def createConsoleSink(chkName: String, mode: OutputMode, df: DataFrame): DataStreamWriter[Row] =
    df
      .writeStream
      .outputMode(mode)
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_8/chk/$chkName")
      .option("truncate", "false")
      .option("numRows", "20")

  val identStreamDf: DataFrame =
    spark
      .readStream
      .format("rate")
      .load()
      .withColumn("ident", getRandomIdent)

  /** Stream-Static - Left Outer join -> Enrichment */
  val rightSideLeftOuterDf: DataFrame = airportsDf()

  val resultLeftOuterDf: DataFrame =
    identStreamDf
      .join(rightSideLeftOuterDf, Seq("ident"), "left")
      .select($"ident", $"name", $"elevation_ft", $"iso_country")

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
    +- Project [ident#51, timestamp#0, value#1L, type#73, name#74, elevation_ft#75, continent#76, iso_country#77, iso_region#78, municipality#79, gps_code#80, iata_code#81, local_code#82, coordinates#83]
       +- Join LeftOuter, (ident#51 = ident#72)
          :- Project [timestamp#0, value#1L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(4643972083273277723))[0] AS ident#51]
          :  +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@6cf633eb, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@4de14c58, [], [timestamp#0, value#1L]
          +- Relation [ident#72,type#73,name#74,elevation_ft#75,continent#76,iso_country#77,iso_region#78,municipality#79,gps_code#80,iata_code#81,local_code#82,coordinates#83] csv

    == Analyzed Logical Plan ==
    ident: string, name: string, elevation_ft: int, iso_country: string
    Project [ident#51, name#74, elevation_ft#75, iso_country#77]
    +- Project [ident#51, timestamp#0, value#1L, type#73, name#74, elevation_ft#75, continent#76, iso_country#77, iso_region#78, municipality#79, gps_code#80, iata_code#81, local_code#82, coordinates#83]
       +- Join LeftOuter, (ident#51 = ident#72)
          :- Project [timestamp#0, value#1L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(4643972083273277723))[0] AS ident#51]
          :  +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@6cf633eb, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@4de14c58, [], [timestamp#0, value#1L]
          +- Relation [ident#72,type#73,name#74,elevation_ft#75,continent#76,iso_country#77,iso_region#78,municipality#79,gps_code#80,iata_code#81,local_code#82,coordinates#83] csv

    == Optimized Logical Plan ==
    Project [ident#51, name#74, elevation_ft#75, iso_country#77]
    +- Join LeftOuter, (ident#51 = ident#72)
       :- Project [shuffle([00A,00AA,00AK,00AL,00AR,00AS,00AZ,00CA,00CL,00CN,00CO,00FA,00FD,00FL,00GA,00GE,00HI,00ID,00IG,00II], Some(6586588463459619913))[0] AS ident#51]
       :  +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@6cf633eb, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@4de14c58, [], [timestamp#0, value#1L]
       +- Project [ident#72, name#74, elevation_ft#75, iso_country#77]
          +- Filter isnotnull(ident#72)
             +- Relation [ident#72,type#73,name#74,elevation_ft#75,continent#76,iso_country#77,iso_region#78,municipality#79,gps_code#80,iata_code#81,local_code#82,coordinates#83] csv

    == Physical Plan ==
    *(2) Project [ident#51, name#74, elevation_ft#75, iso_country#77]
    // BroadcastHashJoin
    +- *(2) BroadcastHashJoin [ident#51], [ident#72], LeftOuter, BuildRight, false
       :- *(2) Project [shuffle([00A,00AA,00AK,00AL,00AR,00AS,00AZ,00CA,00CL,00CN,00CO,00FA,00FD,00FL,00GA,00GE,00HI,00ID,00IG,00II], Some(6586588463459619913))[0] AS ident#51]
       :  +- StreamingRelation rate, [timestamp#0, value#1L]
       +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=123]
          +- *(1) Filter isnotnull(ident#72)
             +- FileScan csv [ident#72,name#74,elevation_ft#75,iso_country#77] Batched: false, DataFilters: [isnotnull(ident#72)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [IsNotNull(ident)], ReadSchema: struct<ident:string,name:string,elevation_ft:int,iso_country:string>
   */

//  createConsoleSink("state12_StreamStatic_LeftJoin", OutputMode.Append, resultLeftOuterDf).start()

  /** Inner join = White list */
  val rightSideInnerDf: DataFrame =
    Vector("00FA", "00IG", "00FD")
      .toDF()
      .withColumnRenamed("value", "ident")

  val resultInnerDf: DataFrame = identStreamDf.join(rightSideInnerDf, Seq("ident"), "inner")
//  createConsoleSink("state13_StreamStatic_InnerJoin", OutputMode.Append, resultInnerDf).start()

  /** Left Anti join = Black list */
  val rightSideLeftAntiDf: DataFrame =
    Vector("00FA", "00IG", "00FD")
      .toDF()
      .withColumnRenamed("value", "ident")

  val resultLeftAnti: DataFrame = identStreamDf.join(rightSideLeftAntiDf, Seq("ident"), "left_anti")
//  createConsoleSink("state14_StreamStatic_LeftAntiJoin", OutputMode.Append, resultLeftAnti).start()


  /** Stream-Stream join */

  /** v1 - Window */
  val leftSideWindowDf: Dataset[Row] =
    spark
      .readStream
      .format("rate")
      .load()
      .withColumn("ident", getRandomIdent)
      .withColumn("left", lit("left"))
      /** !!! вотермарки у соединяемых датафреймов могут отличаться */
      .withWatermark("timestamp", "2 hours")
      /** !!! окна у соединяемых датафреймов должны быть одинаковыми */
      .withColumn("window", window($"timestamp", "1 minute"))
      .as("left")

//  leftSideWindowDf.printSchema()
  /*
    root
     |-- timestamp: timestamp (nullable = true)
     |-- value: long (nullable = true)
     |-- ident: string (nullable = true)
     |-- left: string (nullable = false)
     |-- window: struct (nullable = false)
     |    |-- start: timestamp (nullable = true)
     |    |-- end: timestamp (nullable = true)
   */

  val rightSideWindowDf: Dataset[Row] =
    spark
      .readStream
      .format("rate")
      .load()
      .withColumn("ident", getRandomIdent)
      .withColumn("right", lit("right"))
      .withWatermark("timestamp", "3 hours")
      .withColumn("window", window($"timestamp", "1 minute"))
      .as("right")

//  rightSideWindowDf.printSchema()
  /*
    root
     |-- timestamp: timestamp (nullable = true)
     |-- value: long (nullable = true)
     |-- ident: string (nullable = true)
     |-- right: string (nullable = false)
     |-- window: struct (nullable = false)
     |    |-- start: timestamp (nullable = true)
     |    |-- end: timestamp (nullable = true)
   */

  val joinExprWindow: Column = expr("""left.value = right.value and left.window = right.window""")

  val joinedWindowDf: DataFrame =
    leftSideWindowDf
      .join(rightSideWindowDf, Seq("value", "window"), "inner")
      .select($"window", $"value", $"left", $"right")
//      .join(rightSideWindowDf, joinExprWindow, "inner")
//      .select($"left.window", $"left.value", $"left", $"right")

//  joinedWindowDf.printSchema()
  /*
    root
     |-- window: struct (nullable = false)
     |    |-- start: timestamp (nullable = true)
     |    |-- end: timestamp (nullable = true)
     |-- value: long (nullable = true)
     |-- left: string (nullable = false)
     |-- right: string (nullable = false)
   */

//  joinedWindowDf.explain(true)
  /*
    == Parsed Logical Plan ==
    'Project ['window, 'value, 'left, 'right]
    +- Project [value#136L, window#196-T7200000ms, timestamp#135-T7200000ms, ident#186, left#190, timestamp#203-T10800000ms, ident#254, right#258]
       +- Join Inner, ((value#136L = value#204L) AND (window#196-T7200000ms = window#264-T10800000ms))
          :- SubqueryAlias left
          :  +- Project [timestamp#135-T7200000ms, value#136L, ident#186, left#190, window#197-T7200000ms AS window#196-T7200000ms]
          :     +- Project [named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#197-T7200000ms, timestamp#135-T7200000ms, value#136L, ident#186, left#190]
          :        +- Filter isnotnull(timestamp#135-T7200000ms)
          :           +- EventTimeWatermark timestamp#135: timestamp, 2 hours
          :              +- Project [timestamp#135, value#136L, ident#186, left AS left#190]
          :                 +- Project [timestamp#135, value#136L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(6069140245063857719))[0] AS ident#186]
          :                    +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@2e5337d8, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@ab40f46, [], [timestamp#135, value#136L]
          +- SubqueryAlias right
             +- Project [timestamp#203-T10800000ms, value#204L, ident#254, right#258, window#265-T10800000ms AS window#264-T10800000ms]
                +- Project [named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#265-T10800000ms, timestamp#203-T10800000ms, value#204L, ident#254, right#258]
                   +- Filter isnotnull(timestamp#203-T10800000ms)
                      +- EventTimeWatermark timestamp#203: timestamp, 3 hours
                         +- Project [timestamp#203, value#204L, ident#254, right AS right#258]
                            +- Project [timestamp#203, value#204L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(6785572051185883731))[0] AS ident#254]
                               +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@7740028a, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@409a164d, [], [timestamp#203, value#204L]

    == Analyzed Logical Plan ==
    window: struct<start:timestamp,end:timestamp>, value: bigint, left: string, right: string
    Project [window#196-T7200000ms, value#136L, left#190, right#258]
    +- Project [value#136L, window#196-T7200000ms, timestamp#135-T7200000ms, ident#186, left#190, timestamp#203-T10800000ms, ident#254, right#258]
       +- Join Inner, ((value#136L = value#204L) AND (window#196-T7200000ms = window#264-T10800000ms))
          :- SubqueryAlias left
          :  +- Project [timestamp#135-T7200000ms, value#136L, ident#186, left#190, window#197-T7200000ms AS window#196-T7200000ms]
          :     +- Project [named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#197-T7200000ms, timestamp#135-T7200000ms, value#136L, ident#186, left#190]
          :        +- Filter isnotnull(timestamp#135-T7200000ms)
          :           +- EventTimeWatermark timestamp#135: timestamp, 2 hours
          :              +- Project [timestamp#135, value#136L, ident#186, left AS left#190]
          :                 +- Project [timestamp#135, value#136L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(6069140245063857719))[0] AS ident#186]
          :                    +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@2e5337d8, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@ab40f46, [], [timestamp#135, value#136L]
          +- SubqueryAlias right
             +- Project [timestamp#203-T10800000ms, value#204L, ident#254, right#258, window#265-T10800000ms AS window#264-T10800000ms]
                +- Project [named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#265-T10800000ms, timestamp#203-T10800000ms, value#204L, ident#254, right#258]
                   +- Filter isnotnull(timestamp#203-T10800000ms)
                      +- EventTimeWatermark timestamp#203: timestamp, 3 hours
                         +- Project [timestamp#203, value#204L, ident#254, right AS right#258]
                            +- Project [timestamp#203, value#204L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(6785572051185883731))[0] AS ident#254]
                               +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@7740028a, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@409a164d, [], [timestamp#203, value#204L]

    == Optimized Logical Plan ==
    Project [window#196-T7200000ms, value#136L, left#190, right#258]
    +- Join Inner, ((value#136L = value#204L) AND (window#196-T7200000ms = window#264-T10800000ms))
       :- Project [value#136L, left#190, named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#196-T7200000ms]
       :  +- Filter isnotnull(timestamp#135-T7200000ms)
       :     +- EventTimeWatermark timestamp#135: timestamp, 2 hours
       :        +- Project [timestamp#135, value#136L, left AS left#190]
       :           +- Filter isnotnull(value#136L)
       :              +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@2e5337d8, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@ab40f46, [], [timestamp#135, value#136L]
       +- Project [value#204L, right#258, named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#264-T10800000ms]
          +- Filter isnotnull(timestamp#203-T10800000ms)
             +- EventTimeWatermark timestamp#203: timestamp, 3 hours
                +- Project [timestamp#203, value#204L, right AS right#258]
                   +- Filter isnotnull(value#204L)
                      +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@7740028a, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@409a164d, [], [timestamp#203, value#204L]

    == Physical Plan ==
    *(5) Project [window#196-T7200000ms, value#136L, left#190, right#258]
    // StreamingSymmetricHashJoin (~HashJoin)
    +- StreamingSymmetricHashJoin [value#136L, window#196-T7200000ms], [value#204L, window#264-T10800000ms], Inner, condition = [ leftOnly = null, rightOnly = null, both = null, full = null ], state info [ checkpoint = <unknown>, runId = ae5a7798-96a3-49e9-851c-02a5bee82d6c, opId = 0, ver = 0, numPartitions = 200], -9223372036854775808, -9223372036854775808, state cleanup [ left key predicate: (input[1, struct<start:timestamp,end:timestamp>, false].end <= 0), right key predicate: (input[1, struct<start:timestamp,end:timestamp>, false].end <= 0) ], 2
       :- Exchange hashpartitioning(value#136L, window#196-T7200000ms, 200), ENSURE_REQUIREMENTS, [plan_id=304]
       :  +- *(2) Project [value#136L, left#190, named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#135-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#196-T7200000ms]
       :     +- *(2) Filter isnotnull(timestamp#135-T7200000ms)
       :        +- EventTimeWatermark timestamp#135: timestamp, 2 hours
       :           +- *(1) Project [timestamp#135, value#136L, left AS left#190]
       :              +- *(1) Filter isnotnull(value#136L)
       :                 +- StreamingRelation rate, [timestamp#135, value#136L]
       +- Exchange hashpartitioning(value#204L, window#264-T10800000ms, 200), ENSURE_REQUIREMENTS, [plan_id=314]
          +- *(4) Project [value#204L, right#258, named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#203-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#264-T10800000ms]
             +- *(4) Filter isnotnull(timestamp#203-T10800000ms)
                +- EventTimeWatermark timestamp#203: timestamp, 3 hours
                   +- *(3) Project [timestamp#203, value#204L, right AS right#258]
                      +- *(3) Filter isnotnull(value#204L)
                         +- StreamingRelation rate, [timestamp#203, value#204L]

   */

//  createConsoleSink("state15_StreamStream_Window", OutputMode.Append, joinedWindowDf).start()

  /** v2 - Timestamp */
  val leftTimestampDf: Dataset[Row] =
    spark
      .readStream
      .format("rate")
      .load()
      .withColumn("ident", getRandomIdent)
      .withColumn("left", lit("left"))
      .withWatermark("timestamp", "2 hours")
      .as("left")

  val rightTimestampDf: Dataset[Row] =
    spark
      .readStream
      .format("rate")
      .load()
      .withColumn("ident", getRandomIdent)
      .withColumn("right", lit("right"))
      /** вотермарки не влияют на логику соединения, влияют только на фильтрацию старых дынных */
      .withWatermark("timestamp", "3 hours")
      .as("right")

  /** нужно задать ограничение на таймстэмп */
  val joinExprTimestamp: Column = expr("""left.value = right.value and left.timestamp <= right.timestamp + INTERVAL 1 hour""")

  /** еще один пример */
  val joinExprTimestamp2: Column =
    expr(
      """left.value = right.value
        |and left.timestamp <= right.timestamp + INTERVAL 1 hour
        |and left.timestamp >= right.timestamp - INTERVAL 1 hour
        |""".stripMargin)

  val joinedTimestampDf: DataFrame =
    leftTimestampDf
      .join(rightTimestampDf, joinExprTimestamp, "inner")
      .select($"left.value", $"left.timestamp", $"left", $"right")

  joinedTimestampDf.explain(true)
  /*
    == Parsed Logical Plan ==
    'Project ['left.value, 'left.timestamp, 'left, 'right]
    +- Join Inner, ((value#284L = value#344L) AND (timestamp#283-T7200000ms <= cast(timestamp#343-T10800000ms + INTERVAL '01' HOUR as timestamp)))
       :- SubqueryAlias left
       :  +- EventTimeWatermark timestamp#283: timestamp, 2 hours
       :     +- Project [timestamp#283, value#284L, ident#334, left AS left#338]
       :        +- Project [timestamp#283, value#284L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(92061049035176584))[0] AS ident#334]
       :           +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@39c8101c, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@5f38dc75, [], [timestamp#283, value#284L]
       +- SubqueryAlias right
          +- EventTimeWatermark timestamp#343: timestamp, 3 hours
             +- Project [timestamp#343, value#344L, ident#394, right AS right#398]
                +- Project [timestamp#343, value#344L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-4408509462104480805))[0] AS ident#394]
                   +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@57a22410, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@2e138608, [], [timestamp#343, value#344L]

    == Analyzed Logical Plan ==
    value: bigint, timestamp: timestamp, left: string, right: string
    Project [value#284L, timestamp#283-T7200000ms, left#338, right#398]
    +- Join Inner, ((value#284L = value#344L) AND (timestamp#283-T7200000ms <= cast(timestamp#343-T10800000ms + INTERVAL '01' HOUR as timestamp)))
       :- SubqueryAlias left
       :  +- EventTimeWatermark timestamp#283: timestamp, 2 hours
       :     +- Project [timestamp#283, value#284L, ident#334, left AS left#338]
       :        +- Project [timestamp#283, value#284L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(92061049035176584))[0] AS ident#334]
       :           +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@39c8101c, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@5f38dc75, [], [timestamp#283, value#284L]
       +- SubqueryAlias right
          +- EventTimeWatermark timestamp#343: timestamp, 3 hours
             +- Project [timestamp#343, value#344L, ident#394, right AS right#398]
                +- Project [timestamp#343, value#344L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-4408509462104480805))[0] AS ident#394]
                   +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@57a22410, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@2e138608, [], [timestamp#343, value#344L]

    == Optimized Logical Plan ==
    Project [value#284L, timestamp#283-T7200000ms, left#338, right#398]
    +- Join Inner, ((value#284L = value#344L) AND (timestamp#283-T7200000ms <= timestamp#343-T10800000ms + INTERVAL '01' HOUR))
       :- Filter isnotnull(timestamp#283-T7200000ms)
       :  +- EventTimeWatermark timestamp#283: timestamp, 2 hours
       :     +- Project [timestamp#283, value#284L, left AS left#338]
       :        +- Filter isnotnull(value#284L)
       :           +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@39c8101c, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@5f38dc75, [], [timestamp#283, value#284L]
       +- Filter isnotnull(timestamp#343-T10800000ms)
          +- EventTimeWatermark timestamp#343: timestamp, 3 hours
             +- Project [timestamp#343, value#344L, right AS right#398]
                +- Filter isnotnull(value#344L)
                   +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@57a22410, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@2e138608, [], [timestamp#343, value#344L]

    == Physical Plan ==
    *(5) Project [value#284L, timestamp#283-T7200000ms, left#338, right#398]
    +- StreamingSymmetricHashJoin [value#284L], [value#344L], Inner, condition = [ leftOnly = null, rightOnly = null, both = (timestamp#283-T7200000ms <= timestamp#343-T10800000ms + INTERVAL '01' HOUR), full = (timestamp#283-T7200000ms <= timestamp#343-T10800000ms + INTERVAL '01' HOUR) ], state info [ checkpoint = <unknown>, runId = 206d1fad-501b-46cc-be4d-115e924fdbf4, opId = 0, ver = 0, numPartitions = 200], -9223372036854775808, -9223372036854775808, state cleanup [ left = null, right value predicate: (timestamp#343-T10800000ms <= -1000) ], 2
       :- Exchange hashpartitioning(value#284L, 200), ENSURE_REQUIREMENTS, [plan_id=437]
       :  +- *(2) Filter isnotnull(timestamp#283-T7200000ms)
       :     +- EventTimeWatermark timestamp#283: timestamp, 2 hours
       :        +- *(1) Project [timestamp#283, value#284L, left AS left#338]
       :           +- *(1) Filter isnotnull(value#284L)
       :              +- StreamingRelation rate, [timestamp#283, value#284L]
       +- Exchange hashpartitioning(value#344L, 200), ENSURE_REQUIREMENTS, [plan_id=446]
          +- *(4) Filter isnotnull(timestamp#343-T10800000ms)
             +- EventTimeWatermark timestamp#343: timestamp, 3 hours
                +- *(3) Project [timestamp#343, value#344L, right AS right#398]
                   +- *(3) Filter isnotnull(value#344L)
                      +- StreamingRelation rate, [timestamp#343, value#344L]
   */

//  createConsoleSink("state16_StreamStream_Timestamp", OutputMode.Append, joinedTimestampDf).start()


  Thread.sleep(1_000_000)

  spark.stop()
}
