package l_8

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, lit, shuffle}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object Streaming_5 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_8")
    .getOrCreate

  import spark.implicits._

  def createConsoleSink(chkName: String, df: DataFrame): DataStreamWriter[Row] =
    df
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_8/chk/$chkName")
      .option("truncate", "false")
      .option("numRows", "20")

  def killAllStreams(): Unit = SparkSession
    .active
    .streams
    .active
    .foreach { stream =>
      val description: String = stream
        .lastProgress
        .sources
        .head
        .description

      stream.stop()
      println(s"Stopped $description")
    }

  def getAirportsDf: DataFrame = {
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")
  }

  def getRandomIdent: Column = {
    val idents: Array[String] = getAirportsDf
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

  /** Удаление дубликатов. */
  /** v1 - без использования watermark. */
  val streamDfWithDuplicates: DataFrame = spark
    .readStream
    .format("rate")
    .load()
    .withColumn("ident", getRandomIdent)

//  createConsoleSink("state1_WithDuplicates", streamDfWithDuplicates).start()

  val streamingDfWithoutDuplicates: DataFrame = spark
    .readStream
    .format("rate")
    .load()
    .withColumn("ident", getRandomIdent)
    .dropDuplicates(Seq("ident"))

//  createConsoleSink("state2_WithoutDuplicates", streamingDfWithoutDuplicates).start()

  /** v2 - с использованием watermark. */
  val streamingDfWithoutDuplicatesWatermark: Dataset[Row] = spark
    .readStream
    .format("rate")
    .load
    .withColumn("ident", getRandomIdent)
    /**
     * Задаем watermark и определяем threshold.
     * Фильтр1 - отбрасываем старые данные (в них могли быть и дубликаты).
     * */
    .withWatermark("timestamp", "10 minutes")
    /**
     * Фильтр2 - удаляем дубликаты по полям ident и timestamp + удаляем старые хэши.
     * !!! Удаления дубликатов в этом конкретном примере не происходит т.к. timestamp всегда уникален.
     */
    .dropDuplicates(Seq("ident", "timestamp"))

//  createConsoleSink("state3_WithoutDuplicatesWatermark", streamingDfWithoutDuplicatesWatermark).start()

  /**
   * Если требуется удалять дубликаты в каком-либо временном диапазоне =>
   * => нужно округлять timestamp до необходимого значения.
   */
  val streamingDfWithoutDuplicatesWatermarkRounded: Dataset[Row] = spark
    .readStream
    .format("rate")
    .load
    .withColumn("ident", getRandomIdent)
    /** Удаление дубликатов в рамках одной минуты. */
    .withColumn("timestamp",
      (('timestamp.cast(LongType) / 60).cast(LongType) * 60).cast(TimestampType)
    )
    .withWatermark("timestamp", "10 minutes")
    .dropDuplicates(Seq("ident", "timestamp"))

  createConsoleSink("state4_WithoutDuplicatesWatermarkRound", streamingDfWithoutDuplicatesWatermarkRounded).start()

  Thread.sleep(1000000)
}
