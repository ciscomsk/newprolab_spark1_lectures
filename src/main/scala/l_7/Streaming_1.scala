package l_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, input_file_name, lit, shuffle}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Streaming_1 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_7")
    .getOrCreate

  import spark.implicits._

  /** Стрим, выполняющий запись в консоль. */
  def createConsoleSink(df: DataFrame): DataStreamWriter[Row] =
    df
      .writeStream
      .format("console")
      /** Как часто будут читаться/обрабатываться данные. */
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("truncate", "false")
      .option("numRows", "20")

  val streamDf: DataFrame = spark
    .readStream
    .format("rate")
    .load()

  println(streamDf.isStreaming)
  println()

  /** err - Queries with streaming sources must be executed with writeStream.start() */
//  println(streamDf.rdd.getNumPartitions)
  streamDf.printSchema()

  streamDf.explain(true)
  /*
    == Parsed Logical Plan ==
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@2bc59ab7, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@7507d96c, [], [timestamp#0, value#1L]

    == Analyzed Logical Plan ==
    timestamp: timestamp, value: bigint
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@2bc59ab7, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@7507d96c, [], [timestamp#0, value#1L]

    == Optimized Logical Plan ==
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@2bc59ab7, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@7507d96c, [], [timestamp#0, value#1L]

    == Physical Plan ==
    StreamingRelation rate, [timestamp#0, value#1L]
   */

  val consoleSink: DataStreamWriter[Row] = createConsoleSink(streamDf)
  /** !!! start() - неблокирующая операция. */
//  val streamingQuery: StreamingQuery = consoleSink.start()

  /**
   * !!! AwaitTermination(time) - блокирует основной поток на указанный период времени (time).
   * AwaitTermination(time) - позволяет каждый период времени (time) выполнять нашу логику.
   */

//  val streamIsStopped: Boolean = streamingQuery.awaitTermination(5000)
//  println(streamIsStopped)

//  if (!streamIsStopped) {
    /**
     * v1 - останавливаем стрим.
     * stop - позволяет остановить(возобновить - нельзя)/"убить" стрим.
     */
////    streamingQuery.stop()

    /** v2 - продолжаем блокировать поток. */
//    streamingQuery.awaitTermination()
//  }

  /**
   * Получение всех streaming df и их остановка.
   * !!! Это жесткая остановка - если попасть в момент записи батча (например в БД) - часть данных записана не будет.
   * Чтобы погасить стрим gracefully - надо писать код - конец Streaming_4.
   */
  def killAllStream(): Unit = SparkSession
    .active
    .streams
    .active
    .foreach { stream =>
      val description: String = stream.lastProgress.sources.head.description

      stream.stop()
      println(s"Stopped $description")
    }

  /*
    ./spark-shell

    // Поток не блокируется (3.1.2/3.2.0).
    val sdf = spark.readStream.format("rate").load()
    val sq = sdf.writeStream.format("console").start()

    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    val sq = sdf.writeStream.format("console").start; System.exit(0) // - запустит стрим и выйдет.

    // В 3.1.2 поток блокируется.
    // В 3.2.0 поток не блокируется.
    val sdf = spark.readStream.format("rate").load()
    val sink = sdf.writeStream.format("console")
    val sq = sink.start(); sq.awaitTermination()
   */

  /** Стрим, выполняющий запись в parquet. */
  def createParquetSink(df: DataFrame, fileName: String): DataStreamWriter[Row] =
    df
      .writeStream
      .format("parquet")
      .option("path", s"src/main/resources/l_7/$fileName")
      /** без - err: AnalysisException: checkpointLocation must be specified */
      .option("checkpointLocation", s"src/main/resources/l_7/chk/$fileName")
      .trigger(Trigger.ProcessingTime("10 seconds"))

  val parquetSink: DataStreamWriter[Row] = createParquetSink(streamDf, "s1.parquet")
//  val streamingQuery2: StreamingQuery = parquetSink.start()
//  streamingQuery2.awaitTermination(15000)

//  println(streamingQuery2.isActive)

  /*
    .writeStream
    .queryName("query")
    .start()
   */
//  println(streamingQuery2.name)

//  println(streamingQuery2.lastProgress)
  println()
  /*
    {
      "id" : "e3eec8b6-3322-4aa1-9a16-46337929fbb7",
      "runId" : "c1f8496a-8672-41bb-819b-18778fcfc0d6",
      "name" : null,
      "timestamp" : "2022-01-19T14:24:50.000Z",
      "batchId" : 2,
      "numInputRows" : 10,
      "inputRowsPerSecond" : 1.0,
      "processedRowsPerSecond" : 33.333333333333336,
      "durationMs" : {
        "addBatch" : 258,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 6,
        "triggerExecution" : 300,
        "walCommit" : 15
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 2,
        "endOffset" : 12,
        "latestOffset" : 12,
        "numInputRows" : 10,
        "inputRowsPerSecond" : 1.0,
        "processedRowsPerSecond" : 33.333333333333336
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    }
   */

  /** recentProgress - прогресс по последним микробатчам. */
//  println(streamingQuery2.recentProgress.mkString("Array(", ", ", ")"))
  println()
  /*
    Array({
      "id" : "e3eec8b6-3322-4aa1-9a16-46337929fbb7",
      "runId" : "c1f8496a-8672-41bb-819b-18778fcfc0d6",
      "name" : null,
      "timestamp" : "2022-01-19T14:24:37.429Z",
      "batchId" : 0,
      "numInputRows" : 0,
      "inputRowsPerSecond" : 0.0,
      "processedRowsPerSecond" : 0.0,
      "durationMs" : {
        "addBatch" : 1254,
        "getBatch" : 2,
        "latestOffset" : 0,
        "queryPlanning" : 30,
        "triggerExecution" : 1356,
        "walCommit" : 37
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : null,
        "endOffset" : 0,
        "latestOffset" : 0,
        "numInputRows" : 0,
        "inputRowsPerSecond" : 0.0,
        "processedRowsPerSecond" : 0.0
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    }, {
      "id" : "e3eec8b6-3322-4aa1-9a16-46337929fbb7",
      "runId" : "c1f8496a-8672-41bb-819b-18778fcfc0d6",
      "name" : null,
      "timestamp" : "2022-01-19T14:24:40.000Z",
      "batchId" : 1,
      "numInputRows" : 2,
      "inputRowsPerSecond" : 0.777907429015947,
      "processedRowsPerSecond" : 4.914004914004915,
      "durationMs" : {
        "addBatch" : 362,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 9,
        "triggerExecution" : 407,
        "walCommit" : 18
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 0,
        "endOffset" : 2,
        "latestOffset" : 2,
        "numInputRows" : 2,
        "inputRowsPerSecond" : 0.777907429015947,
        "processedRowsPerSecond" : 4.914004914004915
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    }, {
      "id" : "e3eec8b6-3322-4aa1-9a16-46337929fbb7",
      "runId" : "c1f8496a-8672-41bb-819b-18778fcfc0d6",
      "name" : null,
      "timestamp" : "2022-01-19T14:24:50.000Z",
      "batchId" : 2,
      "numInputRows" : 10,
      "inputRowsPerSecond" : 1.0,
      "processedRowsPerSecond" : 33.333333333333336,
      "durationMs" : {
        "addBatch" : 258,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 6,
        "triggerExecution" : 300,
        "walCommit" : 15
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 2,
        "endOffset" : 12,
        "latestOffset" : 12,
        "numInputRows" : 10,
        "inputRowsPerSecond" : 1.0,
        "processedRowsPerSecond" : 33.333333333333336
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    })
   */

//  println(streamingQuery2.status)
  println()
  /*
    {
      "message" : "Waiting for next trigger",
      "isDataAvailable" : true,
      "isTriggerActive" : false
    }
   */

  val parquetStreamDf: DataFrame = spark.read.load("src/main/resources/l_7/s1.parquet")
  println(parquetStreamDf.count)
  println()

  parquetStreamDf.printSchema()
  parquetStreamDf.show(5, truncate = false)

  /** input_file_name() - позволяет просмотреть файлы, являющиеся источниками датафрейма. */
  parquetStreamDf
    .select(input_file_name())
    .distinct
    .show(20, truncate = false)

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame = spark
    .read
    .options(csvOptions)
    .csv("src/main/resources/l_3/airport-codes.csv")

  airportsDf.printSchema()
  airportsDf.show(numRows = 1, truncate = 100, vertical = true)

  val idents: Array[String] = airportsDf
    .select('ident)
    .limit(200)
    .distinct
    .as[String]
    .collect()

  /**
   * shuffle - перемешивает массив.
   * shuffle(array)(0) - берем первый элемент массива.
    */
  val identStreamDf: DataFrame = streamDf.withColumn("ident", shuffle(array(idents.map(lit): _*))(0))

  val identParquetSink: DataStreamWriter[Row] = createParquetSink(identStreamDf, "s2.parquet")
//  val identStreamQuery: StreamingQuery = identParquetSink.start()
//  identStreamQuery.awaitTermination(15000)

  val identParquetDf: DataFrame = spark
    .read
    .parquet("src/main/resources/l_7/s2.parquet")

  println(identParquetDf.count)
  println()

  identParquetDf.printSchema()
  println(identParquetDf.schema.toDDL)  // v1
  println(identParquetDf.schema.json)  // v2
  println()

  identParquetDf.show(truncate = false)

//  Thread.sleep(1000000)
}
