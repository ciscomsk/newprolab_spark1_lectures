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
      .withColumn("ident", getRandomIdent())

  /** Stream-Static - Left Outer join - Enrichment */
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
          :- Project [timestamp#0, value#1L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(5541539495931103092))[0] AS ident#51]
          :  +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@193f5509, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@475a8415, [], [timestamp#0, value#1L]
          +- Relation [ident#72,type#73,name#74,elevation_ft#75,continent#76,iso_country#77,iso_region#78,municipality#79,gps_code#80,iata_code#81,local_code#82,coordinates#83] csv

    == Analyzed Logical Plan ==
    ident: string, name: string, elevation_ft: int, iso_country: string
    Project [ident#51, name#74, elevation_ft#75, iso_country#77]
    +- Project [ident#51, timestamp#0, value#1L, type#73, name#74, elevation_ft#75, continent#76, iso_country#77, iso_region#78, municipality#79, gps_code#80, iata_code#81, local_code#82, coordinates#83]
       +- Join LeftOuter, (ident#51 = ident#72)
          :- Project [timestamp#0, value#1L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(5541539495931103092))[0] AS ident#51]
          :  +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@193f5509, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@475a8415, [], [timestamp#0, value#1L]
          +- Relation [ident#72,type#73,name#74,elevation_ft#75,continent#76,iso_country#77,iso_region#78,municipality#79,gps_code#80,iata_code#81,local_code#82,coordinates#83] csv

    == Optimized Logical Plan ==
    Project [ident#51, name#74, elevation_ft#75, iso_country#77]
    +- Join LeftOuter, (ident#51 = ident#72)
       :- Project [shuffle([00A,00AA,00AK,00AL,00AR,00AS,00AZ,00CA,00CL,00CN,00CO,00FA,00FD,00FL,00GA,00GE,00HI,00ID,00IG,00II], Some(1453207610205272381))[0] AS ident#51]
       :  +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@193f5509, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@475a8415, [], [timestamp#0, value#1L]
       +- Project [ident#72, name#74, elevation_ft#75, iso_country#77]
          +- Filter isnotnull(ident#72)
             +- Relation [ident#72,type#73,name#74,elevation_ft#75,continent#76,iso_country#77,iso_region#78,municipality#79,gps_code#80,iata_code#81,local_code#82,coordinates#83] csv

    == Physical Plan ==
    *(2) Project [ident#51, name#74, elevation_ft#75, iso_country#77]
    // BroadcastHashJoin
    +- *(2) BroadcastHashJoin [ident#51], [ident#72], LeftOuter, BuildRight, false
       :- *(2) Project [shuffle([00A,00AA,00AK,00AL,00AR,00AS,00AZ,00CA,00CL,00CN,00CO,00FA,00FD,00FL,00GA,00GE,00HI,00ID,00IG,00II], Some(1453207610205272381))[0] AS ident#51]
       :  +- StreamingRelation rate, [timestamp#0, value#1L]
       +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=123]
          +- *(1) Filter isnotnull(ident#72)
             +- FileScan csv [ident#72,name#74,elevation_ft#75,iso_country#77] Batched: false, DataFilters: [isnotnull(ident#72)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [IsNotNull(ident)], ReadSchema: struct<ident:string,name:string,elevation_ft:int,iso_country:string>
   */

//  createConsoleSink("state12_StreamStatic_LeftJoin", OutputMode.Append, resultLeftOuterDf).start()

  /** Inner join - Whitelist */
  val rightSideInnerDf: DataFrame =
    Vector("00FA", "00IG", "00FD")
      .toDF()
      .withColumnRenamed("value", "ident")

  val resultInnerDf: DataFrame = identStreamDf.join(rightSideInnerDf, Seq("ident"), "inner")
//  createConsoleSink("state13_StreamStatic_InnerJoin", OutputMode.Append, resultInnerDf).start()

  /** Left Anti join - Blacklist */
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
      .withColumn("ident", getRandomIdent())
      .withColumn("left", lit("left"))
      /** !!! Ватермарки у соединяемых датафреймов могут отличаться */
      .withWatermark("timestamp", "2 hours")
      /** !!! Окна у соединяемых датафреймов должны быть одинаковыми */
      .withColumn("window", window($"timestamp", "1 minute")).as("left")

  val rightSideWindowDf: Dataset[Row] =
    spark
      .readStream
      .format("rate")
      .load()
      .withColumn("ident", getRandomIdent())
      .withColumn("right", lit("right"))
      .withWatermark("timestamp", "3 hours")
      .withColumn("window", window($"timestamp", "1 minute")).as("right")

  val joinExprWindow: Column = expr("""left.value = right.value and left.window = right.window""")

  val joinedWindowDf: DataFrame =
    leftSideWindowDf
      .join(rightSideWindowDf, joinExprWindow, "inner")
      .select($"left.window", $"left.value", $"left", $"right")

//  joinedWindowDf.explain(true)
  /*
    == Parsed Logical Plan ==
    'Project ['left.window, 'left.value, 'left, 'right]
    +- Join Inner, ((value#139L = value#227L) AND (window#219-T7200000ms = window#287-T10800000ms))
       :- SubqueryAlias left
       :  +- Project [timestamp#138-T7200000ms, value#139L, ident#199, left#209, window#220-T7200000ms AS window#219-T7200000ms]
       :     +- Project [named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#220-T7200000ms, timestamp#138-T7200000ms, value#139L, ident#199, left#209]
       :        +- Filter isnotnull(timestamp#138-T7200000ms)
       :           +- EventTimeWatermark timestamp#138: timestamp, 2 hours
       :              +- Project [timestamp#138, value#139L, ident#199, left AS left#209]
       :                 +- Project [timestamp#138, value#139L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-1618809219499701793))[0] AS ident#199]
       :                    +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@1a2bcce1, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@d4594a8, [], [timestamp#138, value#139L]
       +- SubqueryAlias right
          +- Project [timestamp#226-T10800000ms, value#227L, ident#277, right#281, window#288-T10800000ms AS window#287-T10800000ms]
             +- Project [named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#288-T10800000ms, timestamp#226-T10800000ms, value#227L, ident#277, right#281]
                +- Filter isnotnull(timestamp#226-T10800000ms)
                   +- EventTimeWatermark timestamp#226: timestamp, 3 hours
                      +- Project [timestamp#226, value#227L, ident#277, right AS right#281]
                         +- Project [timestamp#226, value#227L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(257553479276325153))[0] AS ident#277]
                            +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@2dba6013, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@69b2ed81, [], [timestamp#226, value#227L]

    == Analyzed Logical Plan ==
    window: struct<start:timestamp,end:timestamp>, value: bigint, left: string, right: string
    Project [window#219-T7200000ms, value#139L, left#209, right#281]
    +- Join Inner, ((value#139L = value#227L) AND (window#219-T7200000ms = window#287-T10800000ms))
       :- SubqueryAlias left
       :  +- Project [timestamp#138-T7200000ms, value#139L, ident#199, left#209, window#220-T7200000ms AS window#219-T7200000ms]
       :     +- Project [named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#220-T7200000ms, timestamp#138-T7200000ms, value#139L, ident#199, left#209]
       :        +- Filter isnotnull(timestamp#138-T7200000ms)
       :           +- EventTimeWatermark timestamp#138: timestamp, 2 hours
       :              +- Project [timestamp#138, value#139L, ident#199, left AS left#209]
       :                 +- Project [timestamp#138, value#139L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-1618809219499701793))[0] AS ident#199]
       :                    +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@1a2bcce1, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@d4594a8, [], [timestamp#138, value#139L]
       +- SubqueryAlias right
          +- Project [timestamp#226-T10800000ms, value#227L, ident#277, right#281, window#288-T10800000ms AS window#287-T10800000ms]
             +- Project [named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) < cast(0 as bigint)) THEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#288-T10800000ms, timestamp#226-T10800000ms, value#227L, ident#277, right#281]
                +- Filter isnotnull(timestamp#226-T10800000ms)
                   +- EventTimeWatermark timestamp#226: timestamp, 3 hours
                      +- Project [timestamp#226, value#227L, ident#277, right AS right#281]
                         +- Project [timestamp#226, value#227L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(257553479276325153))[0] AS ident#277]
                            +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@2dba6013, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@69b2ed81, [], [timestamp#226, value#227L]

    == Optimized Logical Plan ==
    Project [window#219-T7200000ms, value#139L, left#209, right#281]
    +- Join Inner, ((value#139L = value#227L) AND (window#219-T7200000ms = window#287-T10800000ms))
       :- Project [value#139L, left#209, named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#219-T7200000ms]
       :  +- Filter isnotnull(timestamp#138-T7200000ms)
       :     +- EventTimeWatermark timestamp#138: timestamp, 2 hours
       :        +- Project [timestamp#138, value#139L, left AS left#209]
       :           +- Filter isnotnull(value#139L)
       :              +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@1a2bcce1, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@d4594a8, [], [timestamp#138, value#139L]
       +- Project [value#227L, right#281, named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#287-T10800000ms]
          +- Filter isnotnull(timestamp#226-T10800000ms)
             +- EventTimeWatermark timestamp#226: timestamp, 3 hours
                +- Project [timestamp#226, value#227L, right AS right#281]
                   +- Filter isnotnull(value#227L)
                      +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@2dba6013, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@69b2ed81, [], [timestamp#226, value#227L]

    == Physical Plan ==
    *(5) Project [window#219-T7200000ms, value#139L, left#209, right#281]
    // StreamingSymmetricHashJoin (~HashJoin)
    +- StreamingSymmetricHashJoin [value#139L, window#219-T7200000ms], [value#227L, window#287-T10800000ms], Inner, condition = [ leftOnly = null, rightOnly = null, both = null, full = null ], state info [ checkpoint = <unknown>, runId = 5e790716-282b-4cac-86c3-99c9bb52e166, opId = 0, ver = 0, numPartitions = 200], 0, 0, state cleanup [ left key predicate: (input[1, struct<start:timestamp,end:timestamp>, false].end <= 0), right key predicate: (input[1, struct<start:timestamp,end:timestamp>, false].end <= 0) ], 2
       :- Exchange hashpartitioning(value#139L, window#219-T7200000ms, 200), ENSURE_REQUIREMENTS, [plan_id=407]
       :  +- *(2) Project [value#139L, left#209, named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#138-T7200000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#219-T7200000ms]
       :     +- *(2) Filter isnotnull(timestamp#138-T7200000ms)
       :        +- EventTimeWatermark timestamp#138: timestamp, 2 hours
       :           +- *(1) Project [timestamp#138, value#139L, left AS left#209]
       :              +- *(1) Filter isnotnull(value#139L)
       :                 +- StreamingRelation rate, [timestamp#138, value#139L]
       +- Exchange hashpartitioning(value#227L, window#287-T10800000ms, 200), ENSURE_REQUIREMENTS, [plan_id=417]
          +- *(4) Project [value#227L, right#281, named_struct(start, knownnullable(precisetimestampconversion(((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0), LongType, TimestampType)), end, knownnullable(precisetimestampconversion((((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - CASE WHEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) < 0) THEN (((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) + 60000000) ELSE ((precisetimestampconversion(timestamp#226-T10800000ms, TimestampType, LongType) - 0) % 60000000) END) - 0) + 60000000), LongType, TimestampType))) AS window#287-T10800000ms]
             +- *(4) Filter isnotnull(timestamp#226-T10800000ms)
                +- EventTimeWatermark timestamp#226: timestamp, 3 hours
                   +- *(3) Project [timestamp#226, value#227L, right AS right#281]
                      +- *(3) Filter isnotnull(value#227L)
                         +- StreamingRelation rate, [timestamp#226, value#227L]
   */

//  createConsoleSink("state15_StreamStream_Window", OutputMode.Append, joinedWindowDf).start()

  /** v2 - Timestamp */
  val leftTimestampDf: Dataset[Row] =
    spark
      .readStream
      .format("rate")
      .load()
      .withColumn("ident", getRandomIdent())
      .withColumn("left", lit("left"))
      .withWatermark("timestamp", "2 hours").as("left")

  val rightTimestampDf: Dataset[Row] =
    spark
      .readStream
      .format("rate")
      .load()
      .withColumn("ident", getRandomIdent())
      .withColumn("right", lit("right"))
      /** Ватермарки не влияют на логику соединения, влияют только на фильтрацию старых дынных */
      .withWatermark("timestamp", "3 hours").as("right")

  /** Нужно задать ограничение на таймстэмп */
  val joinExprTimestamp: Column = expr("""left.value = right.value and left.timestamp <= right.timestamp + INTERVAL 1 hour""")

  /** Еще один пример */
  val joinExprTimestamp2: Column =
    expr(
      """left.value = right.value and left.timestamp <= right.timestamp + INTERVAL 1 hour and
        |left.timestamp >= right.timestamp - INTERVAL 1 hour
        |""".stripMargin)

  val joinedTimestampDf: DataFrame =
    leftTimestampDf
      .join(rightTimestampDf, joinExprTimestamp, "inner")
      .select($"left.value", $"left.timestamp", $"left", $"right")

  joinedTimestampDf.explain(true)
  /*
    == Parsed Logical Plan ==
    'Project ['left.value, 'left.timestamp, 'left, 'right]
    +- Join Inner, ((value#296L = value#356L) AND (timestamp#295-T7200000ms <= cast(timestamp#355-T10800000ms + INTERVAL '01' HOUR as timestamp)))
       :- SubqueryAlias left
       :  +- EventTimeWatermark timestamp#295: timestamp, 2 hours
       :     +- Project [timestamp#295, value#296L, ident#346, left AS left#350]
       :        +- Project [timestamp#295, value#296L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-4932542861430660172))[0] AS ident#346]
       :           +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@286e2b71, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@6ad4d333, [], [timestamp#295, value#296L]
       +- SubqueryAlias right
          +- EventTimeWatermark timestamp#355: timestamp, 3 hours
             +- Project [timestamp#355, value#356L, ident#406, right AS right#410]
                +- Project [timestamp#355, value#356L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-3950445620940498236))[0] AS ident#406]
                   +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@4f90b4c9, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@16fc3be9, [], [timestamp#355, value#356L]

    == Analyzed Logical Plan ==
    value: bigint, timestamp: timestamp, left: string, right: string
    Project [value#296L, timestamp#295-T7200000ms, left#350, right#410]
    +- Join Inner, ((value#296L = value#356L) AND (timestamp#295-T7200000ms <= cast(timestamp#355-T10800000ms + INTERVAL '01' HOUR as timestamp)))
       :- SubqueryAlias left
       :  +- EventTimeWatermark timestamp#295: timestamp, 2 hours
       :     +- Project [timestamp#295, value#296L, ident#346, left AS left#350]
       :        +- Project [timestamp#295, value#296L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-4932542861430660172))[0] AS ident#346]
       :           +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@286e2b71, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@6ad4d333, [], [timestamp#295, value#296L]
       +- SubqueryAlias right
          +- EventTimeWatermark timestamp#355: timestamp, 3 hours
             +- Project [timestamp#355, value#356L, ident#406, right AS right#410]
                +- Project [timestamp#355, value#356L, shuffle(array(00A, 00AA, 00AK, 00AL, 00AR, 00AS, 00AZ, 00CA, 00CL, 00CN, 00CO, 00FA, 00FD, 00FL, 00GA, 00GE, 00HI, 00ID, 00IG, 00II), Some(-3950445620940498236))[0] AS ident#406]
                   +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@4f90b4c9, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@16fc3be9, [], [timestamp#355, value#356L]

    == Optimized Logical Plan ==
    Project [value#296L, timestamp#295-T7200000ms, left#350, right#410]
    +- Join Inner, ((value#296L = value#356L) AND (timestamp#295-T7200000ms <= timestamp#355-T10800000ms + INTERVAL '01' HOUR))
       :- Filter isnotnull(timestamp#295-T7200000ms)
       :  +- EventTimeWatermark timestamp#295: timestamp, 2 hours
       :     +- Project [timestamp#295, value#296L, left AS left#350]
       :        +- Filter isnotnull(value#296L)
       :           +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@286e2b71, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@6ad4d333, [], [timestamp#295, value#296L]
       +- Filter isnotnull(timestamp#355-T10800000ms)
          +- EventTimeWatermark timestamp#355: timestamp, 3 hours
             +- Project [timestamp#355, value#356L, right AS right#410]
                +- Filter isnotnull(value#356L)
                   +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@4f90b4c9, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@16fc3be9, [], [timestamp#355, value#356L]

    == Physical Plan ==
    *(5) Project [value#296L, timestamp#295-T7200000ms, left#350, right#410]
    +- StreamingSymmetricHashJoin [value#296L], [value#356L], Inner, condition = [ leftOnly = null, rightOnly = null, both = (timestamp#295-T7200000ms <= timestamp#355-T10800000ms + INTERVAL '01' HOUR), full = (timestamp#295-T7200000ms <= timestamp#355-T10800000ms + INTERVAL '01' HOUR) ], state info [ checkpoint = <unknown>, runId = 2af3fbb5-add3-46e0-9531-8d36ccc80da6, opId = 0, ver = 0, numPartitions = 200], 0, 0, state cleanup [ left = null, right value predicate: (timestamp#355-T10800000ms <= -3600001000) ], 2
       :- Exchange hashpartitioning(value#296L, 200), ENSURE_REQUIREMENTS, [plan_id=435]
       :  +- *(2) Filter isnotnull(timestamp#295-T7200000ms)
       :     +- EventTimeWatermark timestamp#295: timestamp, 2 hours
       :        +- *(1) Project [timestamp#295, value#296L, left AS left#350]
       :           +- *(1) Filter isnotnull(value#296L)
       :              +- StreamingRelation rate, [timestamp#295, value#296L]
       +- Exchange hashpartitioning(value#356L, 200), ENSURE_REQUIREMENTS, [plan_id=444]
          +- *(4) Filter isnotnull(timestamp#355-T10800000ms)
             +- EventTimeWatermark timestamp#355: timestamp, 3 hours
                +- *(3) Project [timestamp#355, value#356L, right AS right#410]
                   +- *(3) Filter isnotnull(value#356L)
                      +- StreamingRelation rate, [timestamp#355, value#356L]
   */

//  createConsoleSink("state16_StreamStream_Timestamp", OutputMode.Append, joinedTimestampDf).start()


  Thread.sleep(1000000)

  spark.stop()
}
