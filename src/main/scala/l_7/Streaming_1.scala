package l_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{array, input_file_name, lit, log, shuffle}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object Streaming_1 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_7")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")
  println(sc.uiWebUrl)
  println()

  import spark.implicits._

  /** стрим - выполняет запись в консоль */
  def createConsoleSink(df: DataFrame): DataStreamWriter[Row] = {
    df
      .writeStream
      .format("console")
      /** trigger - как часто будет производится чтение из источника */
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("truncate", "false")
      .option("numRows", "20")
  }

  val rateStreamDf: DataFrame =
    spark
      .readStream
      /** 1 раз в секунду генерирует событие - [timestamp, value]  */
      .format("rate")
      .load()

//  println(s"rateStreamDf.isStreaming: ${rateStreamDf.isStreaming}")
  println()

  /**
   * err - org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start()
   * https://stackoverflow.com/questions/59608106/how-to-transform-dataframes-to-rdds-in-structured-streaming
   */
//  println(rateStreamDf.rdd.getNumPartitions)

//  rateStreamDf.printSchema()
  /*
    root
     |-- timestamp: timestamp (nullable = true)
     |-- value: long (nullable = true)
   */

//  rateStreamDf.explain(true)
  /*
    == Parsed Logical Plan ==
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@37fca349, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@77998ea7, [], [timestamp#0, value#1L]

    == Analyzed Logical Plan ==
    timestamp: timestamp, value: bigint
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@37fca349, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@77998ea7, [], [timestamp#0, value#1L]

    == Optimized Logical Plan ==
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@37fca349, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@77998ea7, [], [timestamp#0, value#1L]

    == Physical Plan ==
    StreamingRelation rate, [timestamp#0, value#1L]
   */

  val consoleSink: DataStreamWriter[Row] = createConsoleSink(rateStreamDf)
  /** !!! start() - запуск стрима, неблокирующая операция */
//  val streamingQuery: StreamingQuery = consoleSink.start()
//  Thread.sleep(12000)
  /** stop - остановка стрима */
//  streamingQuery.stop()

  /**
   * !!! AwaitTermination(time) - блокирует основной поток на указанный период времени <time>
   * позволяет каждый период времени <time> выполнять нашу логику
   */
//  val streamIsStopped: Boolean = streamingQuery.awaitTermination(5000)
//  println(s"streamIsStopped: $streamIsStopped")
//  val stopStream: Boolean = true
//  println()
//
//  if (!streamIsStopped) {
//    if (stopStream) {
//      println("stop streaming")
//      /** stop - остановка стрима */
//      streamingQuery.stop()
//    } else {
//      println("continue streaming")
//      /** продолжаем блокировать поток */
//      streamingQuery.awaitTermination()
//    }
//  }

//  while (true) {
//    Try {
//      streamingQuery.awaitTermination(5000)
//    } match {
//      case Failure(ex) =>
//        val sq: StreamingQuery = consoleSink.start()
//        sq.awaitTermination(5000)
//
//      case Success(value) =>
//    }
//  }

  /** мои попытки с tailrec */
//  @tailrec
//  def endlessAwaitTermination(sq: StreamingQuery): Unit = {
//    sq.awaitTermination(5000)
//    endlessAwaitTermination(sq)
//  }

//  @tailrec
//  def streamErrorRecovery[T](dsw: DataStreamWriter[T]): Unit = {
//    Try {
//      val sq: StreamingQuery = dsw.start()
//      sq.awaitTermination(5000)
//    } match {
//      case Failure(ex) =>
//        streamErrorRecovery(dsw)
//
//      case Success(value) =>
//        endlessAwaitTermination()
//    }
//  }

  /**
   * Остановка всех стримов
   * !!! это жесткая остановка - если попасть в момент записи батча (например в БД) -> часть данных записана не будет
   *
   * чтобы безопасно (gracefully) остановить стрим нужен дополнительный код -> конец Streaming_4
   */
  def killAllStream(): Unit =
    SparkSession
      .active
      .streams
      .active
      .foreach { stream =>
        val description: String =
          stream
            .lastProgress
            .sources
            .head
            .description

        stream.stop()
        println(s"Stopped: $description")
      }

//  killAllStream()

  /*
    ./spark-shell

    val sdf = spark.readStream.format("rate").load()
    val sq = sdf.writeStream.format("console").start()
    // поток не блокируется
    System.exit(0) - завершит основной поток программы

    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    val sdf = spark.readStream.format("rate").load()
    val sq = sdf.writeStream.format("console").start(); System.exit(0) // - запустит стрим и выйдет


    val sdf = spark.readStream.format("rate").load()
    val sq = sdf.writeStream.format("console").start(); sq.awaitTermination()
    // sq.awaitTermination() - поток блокируется
    System.exit(0) // реакции не будет

    val sdf = spark.readStream.format("rate").load()
    val sq = sdf.writeStream.format("console").start(); val sqRes = sq.awaitTermination(5000); println(s"sqRes: $sqRes")
   */

  /** стрим - выполняет запись в parquet */
  def createParquetSink(df: DataFrame, streamName: String): DataStreamWriter[Row] =
    df
      .writeStream
      .queryName("rate-parquet")
      .format("parquet")
      .option("path", s"src/main/resources/l_7/$streamName")
      /**
      * без checkpointLocation - err:
      * AnalysisException: checkpointLocation must be specified either through option("checkpointLocation", ...) or SparkSession.conf.set("spark.sql.streaming.checkpointLocation", ...)
      */
      .option("checkpointLocation", s"src/main/resources/l_7/chk/$streamName")
      .trigger(Trigger.ProcessingTime("10 seconds"))

//  val parquetSink: DataStreamWriter[Row] = createParquetSink(rateStreamDf, "s1.parquet")
//  val parquetSink: DataStreamWriter[Row] = createParquetSink(rateStreamDf.repartition(1), "s1.parquet_repartition")

//  val streamingQuery2: StreamingQuery = parquetSink.start()
//  streamingQuery2.awaitTermination(15000)
//  println(s"streamingQuery2.isActive: ${streamingQuery2.isActive}")
//  println(s"streamingQuery2.name: ${streamingQuery2.name}")
//  println()

//  println(s"streamingQuery2.lastProgress: \n${streamingQuery2.lastProgress}")
  /*
    {
      "id" : "a9e00bc7-cd12-4173-94ad-3cfe0139bbc3",
      "runId" : "1f6defee-f412-4e72-9d0b-39b101069182",
      "name" : "rate-parquet",
      "timestamp" : "2024-05-17T16:27:50.000Z",
      "batchId" : 2,
      "numInputRows" : 9,
      "inputRowsPerSecond" : 0.9784735812133072,
      "processedRowsPerSecond" : 41.28440366972477,
      "durationMs" : {
        "addBatch" : 134,
        "commitOffsets" : 24,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 8,
        "triggerExecution" : 218,
        "walCommit" : 50
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 1,
        "endOffset" : 10,
        "latestOffset" : 10,
        "numInputRows" : 9,
        "inputRowsPerSecond" : 0.9784735812133072,
        "processedRowsPerSecond" : 41.28440366972477
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    }
   */
//  println()

  /** recentProgress - прогресс по последним микробатчам */
//  println(s"streamingQuery2.recentProgress.mkString(\"Array(\", \", \", \")\": ")
//  println(streamingQuery2.recentProgress.mkString("Array(", ", ", ")"))
  /*
    Array({
      "id" : "a9e00bc7-cd12-4173-94ad-3cfe0139bbc3",
      "runId" : "1f6defee-f412-4e72-9d0b-39b101069182",
      "name" : "rate-parquet",
      "timestamp" : "2024-05-17T16:27:39.252Z",
      "batchId" : 0,
      "numInputRows" : 0,
      "inputRowsPerSecond" : 0.0,
      "processedRowsPerSecond" : 0.0,
      "durationMs" : {
        "addBatch" : 1123,
        "commitOffsets" : 20,
        "getBatch" : 3,
        "latestOffset" : 0,
        "queryPlanning" : 325,
        "triggerExecution" : 1536,
        "walCommit" : 38
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
      "id" : "a9e00bc7-cd12-4173-94ad-3cfe0139bbc3",
      "runId" : "1f6defee-f412-4e72-9d0b-39b101069182",
      "name" : "rate-parquet",
      "timestamp" : "2024-05-17T16:27:40.802Z",
      "batchId" : 1,
      "numInputRows" : 1,
      "inputRowsPerSecond" : 0.6451612903225806,
      "processedRowsPerSecond" : 2.242152466367713,
      "durationMs" : {
        "addBatch" : 383,
        "commitOffsets" : 20,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 16,
        "triggerExecution" : 446,
        "walCommit" : 22
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 0,
        "endOffset" : 1,
        "latestOffset" : 1,
        "numInputRows" : 1,
        "inputRowsPerSecond" : 0.6451612903225806,
        "processedRowsPerSecond" : 2.242152466367713
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    }, {
      "id" : "a9e00bc7-cd12-4173-94ad-3cfe0139bbc3",
      "runId" : "1f6defee-f412-4e72-9d0b-39b101069182",
      "name" : "rate-parquet",
      "timestamp" : "2024-05-17T16:27:50.000Z",
      "batchId" : 2,
      "numInputRows" : 9,
      "inputRowsPerSecond" : 0.9784735812133072,
      "processedRowsPerSecond" : 41.28440366972477,
      "durationMs" : {
        "addBatch" : 134,
        "commitOffsets" : 24,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 8,
        "triggerExecution" : 218,
        "walCommit" : 50
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 1,
        "endOffset" : 10,
        "latestOffset" : 10,
        "numInputRows" : 9,
        "inputRowsPerSecond" : 0.9784735812133072,
        "processedRowsPerSecond" : 41.28440366972477
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    })
   */
//  println()

//  println(s"streamingQuery2.status: \n${streamingQuery2.status}")
  /*
    {
      "message" : "Waiting for next trigger",
      "isDataAvailable" : true,
      "isTriggerActive" : false
    }
   */
//  println()

//  val parquetStreamDf: DataFrame = {
//    spark.read.load("src/main/resources/l_7/s1.parquet")
////    spark.read.load("src/main/resources/l_7/s1.parquet_repartition")
//  }
//
//  println(parquetStreamDf.count())
//  println()

//  parquetStreamDf.printSchema()
  /*
    root
     |-- timestamp: timestamp (nullable = true)
     |-- value: long (nullable = true)
   */
//  parquetStreamDf.show(5, truncate = false)

  /** input_file_name() - позволяет просмотреть файлы, являющиеся источниками датафрейма */
//  val uniqFilesDs: Dataset[Row] =
//    parquetStreamDf
//      .select(input_file_name())
//      .distinct()
//
//  uniqFilesDs.show(20, truncate = false)
//  println(uniqFilesDs.count())
//  println()

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

  airportsDf.printSchema()
  airportsDf.show(numRows = 1, truncate = 100, vertical = true)
  println()

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
   * shuffle(array)(0) - берет первый элемент случайно перемешанного массива
    */
  val identStreamDf: DataFrame = rateStreamDf.withColumn("ident", shuffle(array(idents.map(lit): _*))(0))

  val identParquetSink: DataStreamWriter[Row] = createParquetSink(identStreamDf, "s2.parquet")
//  val identStreamQuery: StreamingQuery = identParquetSink.start()
//  identStreamQuery.awaitTermination(15000)

//  val identParquetDf: DataFrame =
//    spark
//      .read
//      .parquet("src/main/resources/l_7/s2.parquet")
//
//  println(s"identParquetDf.count(): ${identParquetDf.count()}")
//  println()

//  identParquetDf.printSchema()
  /*
    root
     |-- timestamp: timestamp (nullable = true)
     |-- value: long (nullable = true)
     |-- ident: string (nullable = true)
   */
//  println(identParquetDf.schema.toDDL)
  // timestamp TIMESTAMP,value BIGINT,ident STRING
//  println(identParquetDf.schema.json)
  // {"type":"struct","fields":[{"name":"timestamp","type":"timestamp","nullable":true,"metadata":{}},{"name":"value","type":"long","nullable":true,"metadata":{}},{"name":"ident","type":"string","nullable":true,"metadata":{}}]}
//  println()

//  identParquetDf.show(truncate = false)


  println(sc.uiWebUrl)
  Thread.sleep(1_000_000)

  spark.stop()
}
