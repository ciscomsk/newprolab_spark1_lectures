package l_8

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, current_timestamp, date_sub, lit, shuffle, split, window}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, Row, SparkSession}

object Streaming_6 extends App {
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

  def getRandomIdent: Column = {
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

  def createConsoleSink(chkName: String, mode: OutputMode, df: DataFrame): DataStreamWriter[Row] =
    df
      .writeStream
      .outputMode(mode)
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_8/chk/$chkName")
      .option("truncate", "false")
      .option("numRows", "20")

  val streamDfWithDuplicates: DataFrame = spark
    .readStream
    .format("rate")
    .load()
    .withColumn("ident", getRandomIdent)

  /**
   * Complete mode.
   * Таблица с полным агрегатом по ВСЕМУ стриму (рассчитывается по всем батчам в стриме), обновляемая на каждом батче.
   */
  val groupedDf: DataFrame = streamDfWithDuplicates
    /**
     * Можно уменьшить кардинальность ключа до 26 (количество букв в английском алфавите).
     * split('ident, "")(2) => 00GA => G
     */
//    .select(split('ident, "")(2).as("ident"))
    .groupBy('ident)
    .count()

//  createConsoleSink("state5_CompleteAgg", OutputMode.Complete, groupedDf).start()

  /**
   * Update mode.
   * Получаем только дельты (изменившиеся строки).
   */
//  createConsoleSink("state6_UpdateAgg", OutputMode.Update, groupedDf).start()

  /** Update mode + watermark. */
  /** v1 - watermark + группировка по указанным колонкам. */
  val groupedWithWatermarkDf: DataFrame = streamDfWithDuplicates
    .withColumn("timestamp",
      (('timestamp.cast(LongType) / 3600).cast(LongType) * 3600).cast(TimestampType)
    )
    .withWatermark("timestamp", "2 hour")
    .groupBy('ident, 'timestamp)
    .count()

//  createConsoleSink("state7_UpdateAggColWatermark", OutputMode.Update, groupedWithWatermarkDf).start()

  /** v2 - watermark + группировка по sliding window. */
  val oldDateDf: DataFrame = spark
    .readStream
    .format("rate")
    .load()
    .withColumn("ident", lit("OLD_DATA"))
    .withColumn("timestamp", date_sub('timestamp, 1))

//  createConsoleSink("state8_Example", OutputMode.Append, oldDateDf).start()

  val newDataDf: DataFrame = spark
    .readStream
    .format("rate")
    .load()
    .withColumn("ident", getRandomIdent)

  val unionDataDf: DataFrame = newDataDf
    .union(oldDateDf)
    .withWatermark("timestamp", "10 minutes")
    /** window('timestamp, "10 minutes" - ширина окна, "5 minutes" - пересечение окон). */
    .groupBy(window('timestamp, "10 minutes"), 'ident)
    .count()

  /**
   * !!! В первом батче будет OLD_DATA, т.к. в нулевом батче MOT не был установлен из-за отсутствия данных в нем.
   * В последующих - не будет из-за watermark.
   */
//  createConsoleSink("state9_UpdateAggWindowWatermark", OutputMode.Update, unionDataDf).start()

  /** Окна доступны и для статических датафреймов. */
//  spark
//    .range(10)
//    .select(current_timestamp.as("ts"))
//    .select(
//      'ts,
//      window('ts, "10 minutes", "5 minutes").as("win")
//    )
//    .show(40, truncate = false)


  /** Append mode. Без  watermark - append режим не работает.*/
  /**
   * !!! В append режиме в синк будут записаны ТОЛЬКО ЗАВЕРШЕННЫЕ окна с данными в момент window_right_bound + watermark_value.
   * Первый результат будет через ~1.5 минуты (window duration + delayThreshold).
   */
  val unionDataDf2: DataFrame = newDataDf
    .withWatermark("timestamp", "1 minutes")
    .groupBy(window('timestamp, "30 seconds"))
    .count()

//  createConsoleSink("state10_AppendAgg_1", OutputMode.Append, unionDataDf2).start()

  /** Более наглядный пример с append mode. */
  val unionDataDf3: DataFrame = newDataDf
    .withColumn("timestamp",
      (('timestamp.cast(LongType) / 60).cast(LongType) * 60).cast("timestamp")
    )
    .withWatermark("timestamp", "1 minutes")
    .groupBy('ident, 'timestamp)
    .count()

//  createConsoleSink("state11_AppendAgg_2", OutputMode.Append, unionDataDf3).start()

  Thread.sleep(1000000)
}
