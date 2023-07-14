package l_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{array, input_file_name, lit, shuffle}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.{Failure, Try, Success}

object Streaming_1 extends App {
  // не работает в Spark 3.4.0
//  Logger
//    .getLogger("org")
//    .setLevel(Level.OFF)

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_7")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")
  println(sc.uiWebUrl)
  println()

  import spark.implicits._

  /** Стрим - выполняет запись в консоль */
  def createConsoleSink(df: DataFrame): DataStreamWriter[Row] =
    df
      .writeStream
      .format("console")
      /** Как часто будут читаться/обрабатываться данные */
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("truncate", "false")
      .option("numRows", "20")

  val rateStreamDf: DataFrame =
    spark
      .readStream
      .format("rate")
      .load()

  println(rateStreamDf.isStreaming)
  println()

  /** err - Queries with streaming sources must be executed with writeStream.start() */
//  println(streamDf.rdd.getNumPartitions)
  rateStreamDf.printSchema()
  /*
    root
     |-- timestamp: timestamp (nullable = true)
     |-- value: long (nullable = true)
   */

  rateStreamDf.explain(true)
  /*
    == Parsed Logical Plan ==
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@36fe83d, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@5cb6abc8, [], [timestamp#0, value#1L]

    == Analyzed Logical Plan ==
    timestamp: timestamp, value: bigint
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@36fe83d, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@5cb6abc8, [], [timestamp#0, value#1L]

    == Optimized Logical Plan ==
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@36fe83d, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@5cb6abc8, [], [timestamp#0, value#1L]

    == Physical Plan ==
    StreamingRelation rate, [timestamp#0, value#1L]
   */

  val consoleSink: DataStreamWriter[Row] = createConsoleSink(rateStreamDf)
  /** !!! start() - запуск стрима, неблокирующая операция */
//  val streamingQuery: StreamingQuery = consoleSink.start()

  /**
   * !!! AwaitTermination(time) - блокирует основной поток на указанный период времени time
   * AwaitTermination(time) - позволяет каждый период времени time выполнять нашу логику
   */

//  val streamIsStopped: Boolean = streamingQuery.awaitTermination(5000)
//  println(s"streamIsStopped: $streamIsStopped")
//  println()

//  if (!streamIsStopped) {
//    /**
//     * останавливаем стрим
//     * stop - позволяет остановить(возобновить - нельзя), т.е. "убить" стрим
//     */
//    println("stop streaming")
//    streamingQuery.stop()
//
//    /** продолжаем блокировать поток */
//    println("continue streaming")
//    streamingQuery.awaitTermination()
//  }

//  while (true) {
//    Try {
//      streamingQuery.awaitTermination(5000)
//    } match {
//      case Failure(exception) => ???
//      case Success(value) => ???
//    }
//  }


  /**
   * Остановка всех стримов
   * !!! Это жесткая остановка - если попасть в момент записи батча (например в БД) - часть данных записана не будет
   * Чтобы погасить стрим gracefully - надо писать код - конец Streaming_4
   */
  def killAllStream(): Unit =
    SparkSession
      .active
      .streams
      .active
      .foreach { stream =>
        val description: String = stream.lastProgress.sources.head.description

        stream.stop()
        println(s"Stopped $description")
      }

//  killAllStream()

  /*
    ./spark-shell

    val sdf = spark.readStream.format("rate").load()
    val sq = sdf.writeStream.format("console").start()
    // Поток не блокируется - 3.4.0
    System.exit(0)

    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    val sq = sdf.writeStream.format("console").start; System.exit(0) // - запустит стрим и выйдет


    val sdf = spark.readStream.format("rate").load()
    val sink = sdf.writeStream.format("console")
    val sq = sink.start(); sq.awaitTermination()
    // sq.awaitTermination() - поток блокируется - 3.4.0
    // System.exit(0) - реакции не будет

    val sq = sink.start(); val sqRes = sq.awaitTermination(5000); println(s"sqRes: $sqRes")
   */

  /** Стрим - выполняет запись в parquet */
  def createParquetSink(df: DataFrame, fileName: String): DataStreamWriter[Row] =
    df
      .writeStream
      .queryName("rate-parquet")
      .format("parquet")
      .option("path", s"src/main/resources/l_7/$fileName")
      /** без - err: AnalysisException: checkpointLocation must be specified */
      .option("checkpointLocation", s"src/main/resources/l_7/chk/$fileName")
      .trigger(Trigger.ProcessingTime("10 seconds"))

  val parquetSink: DataStreamWriter[Row] = createParquetSink(rateStreamDf, "s1.parquet")
//  val parquetSink: DataStreamWriter[Row] = createParquetSink(rateStreamDf.repartition(1), "s1.parquet")
//  val streamingQuery2: StreamingQuery = parquetSink.start()
//  streamingQuery2.awaitTermination(15000)
//  println(s"streamingQuery2.isActive: ${streamingQuery2.isActive}")
//  println(s"streamingQuery2.name: ${streamingQuery2.name}")
//  println()

//  println(s"streamingQuery2.lastProgress: ")
//  println(streamingQuery2.lastProgress)
  /*
    {
      "id" : "a7f65da5-5105-4c28-8556-f56dfc54338a",
      "runId" : "dcb839ac-a3ff-4854-bc8d-82080f9da55d",
      "name" : "rate-parquet",
      "timestamp" : "2023-04-16T13:36:20.000Z",
      "batchId" : 7,
      "numInputRows" : 9,
      "inputRowsPerSecond" : 1.0034563496487903,
      "processedRowsPerSecond" : 51.13636363636364,
      "durationMs" : {
        "addBatch" : 118,
        "commitOffsets" : 17,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 8,
        "triggerExecution" : 176,
        "walCommit" : 29
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 189,
        "endOffset" : 198,
        "latestOffset" : 198,
        "numInputRows" : 9,
        "inputRowsPerSecond" : 1.0034563496487903,
        "processedRowsPerSecond" : 51.13636363636364
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    }
   */
  println()

  /** recentProgress - прогресс по последним микробатчам */
//  println(s"streamingQuery2.recentProgress.mkString(\"Array(\", \", \", \")\": ")
//  println(streamingQuery2.recentProgress.mkString("Array(", ", ", ")"))
  /*
    Array({
      "id" : "a7f65da5-5105-4c28-8556-f56dfc54338a",
      "runId" : "2a70e96f-bd09-4ce2-b792-bfefe31b03fd",
      "name" : "rate-parquet",
      "timestamp" : "2023-04-16T13:41:14.037Z",
      "batchId" : 36,
      "numInputRows" : 14,
      "inputRowsPerSecond" : 0.0,
      "processedRowsPerSecond" : 8.588957055214724,
      "durationMs" : {
        "addBatch" : 1457,
        "commitOffsets" : 17,
        "getBatch" : 2,
        "latestOffset" : 0,
        "queryPlanning" : 28,
        "triggerExecution" : 1629,
        "walCommit" : 44
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 478,
        "endOffset" : 492,
        "latestOffset" : 492,
        "numInputRows" : 14,
        "inputRowsPerSecond" : 0.0,
        "processedRowsPerSecond" : 8.588957055214724
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    }, {
      "id" : "a7f65da5-5105-4c28-8556-f56dfc54338a",
      "runId" : "2a70e96f-bd09-4ce2-b792-bfefe31b03fd",
      "name" : "rate-parquet",
      "timestamp" : "2023-04-16T13:41:20.000Z",
      "batchId" : 37,
      "numInputRows" : 6,
      "inputRowsPerSecond" : 1.006204930404159,
      "processedRowsPerSecond" : 20.833333333333336,
      "durationMs" : {
        "addBatch" : 180,
        "commitOffsets" : 29,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 16,
        "triggerExecution" : 288,
        "walCommit" : 57
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 492,
        "endOffset" : 498,
        "latestOffset" : 498,
        "numInputRows" : 6,
        "inputRowsPerSecond" : 1.006204930404159,
        "processedRowsPerSecond" : 20.833333333333336
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    })
   */
  println()

//  println(s"streamingQuery2.status: ")
//  println(streamingQuery2.status)
  /*
    {
      "message" : "Waiting for next trigger",
      "isDataAvailable" : true,
      "isTriggerActive" : false
    }
   */
  println()

//  val parquetStreamDf: DataFrame = spark.read.load("src/main/resources/l_7/s1.parquet")
//  println(parquetStreamDf.count())
//  println()
//
//  parquetStreamDf.printSchema()
//  parquetStreamDf.show(5, truncate = false)

  /** input_file_name() - позволяет просмотреть файлы, являющиеся источниками датафрейма */
//  parquetStreamDf
//    .select(input_file_name())
//    .distinct()
////    .count()
//    .show(20, truncate = false)

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

//  airportsDf.printSchema()
//  airportsDf.show(numRows = 1, truncate = 100, vertical = true)

  val idents: Array[String] =
    airportsDf
      .select($"ident")
      .limit(200)
      .distinct()
      .as[String]
      .collect()

  println(s"idents: ${idents.mkString("Array(", ", ", ")")}")
  println()

  /**
   * shuffle - перемешивает массив
   * shuffle(array)(0) - берет первый элемент массива
    */
  val identStreamDf: DataFrame = rateStreamDf.withColumn("ident", shuffle(array(idents.map(lit): _*))(0))

  val identParquetSink: DataStreamWriter[Row] = createParquetSink(identStreamDf, "s2.parquet")
//  val identStreamQuery: StreamingQuery = identParquetSink.start()
//  identStreamQuery.awaitTermination(15000)

  val identParquetDf: DataFrame =
    spark
      .read
      .parquet("src/main/resources/l_7/s2.parquet")

  println(identParquetDf.count())
  println()

  identParquetDf.printSchema()
  println(identParquetDf.schema.toDDL)
  println(identParquetDf.schema.json)
  println()

  identParquetDf.show(truncate = false)


  Thread.sleep(1000000)

  spark.stop()
}
