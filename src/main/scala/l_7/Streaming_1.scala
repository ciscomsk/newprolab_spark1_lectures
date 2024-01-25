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
      .format("rate")
      .load()

  println(s"rateStreamDf.isStreaming: ${rateStreamDf.isStreaming}") // == true
  println()

  /** err - org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start() */
//  println(rateStreamDf.rdd.getNumPartitions)
  rateStreamDf.printSchema()
  /*
    root
     |-- timestamp: timestamp (nullable = true)
     |-- value: long (nullable = true)
   */

  rateStreamDf.explain(true)
  /*
    == Parsed Logical Plan ==
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@1fcc3461, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@41bbb219, [], [timestamp#0, value#1L]

    == Analyzed Logical Plan ==
    timestamp: timestamp, value: bigint
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@1fcc3461, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@41bbb219, [], [timestamp#0, value#1L]

    == Optimized Logical Plan ==
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@1fcc3461, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@41bbb219, [], [timestamp#0, value#1L]

    == Physical Plan ==
    StreamingRelation rate, [timestamp#0, value#1L]
   */

  val consoleSink: DataStreamWriter[Row] = createConsoleSink(rateStreamDf)
  /** !!! start() - запуск стрима, неблокирующая операция */
//  val streamingQuery: StreamingQuery = consoleSink.start()
//  Thread.sleep(12000L)
  /** stop - остановка стрима */
//  streamingQuery.stop()

  /**
   * !!! AwaitTermination(time) - блокирует основной поток на указанный период времени time
   * AwaitTermination(time) - позволяет каждый период времени time выполнять нашу логику
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
//    }
//
//    println("continue streaming")
//    /** продолжаем блокировать поток */
//    streamingQuery.awaitTermination()
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

  /** Мои попытки с tailrec */
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
   * !!! это жесткая остановка - если попасть в момент записи батча (например в БД) => часть данных записана не будет
   *
   * Чтобы безопасно (gracefully) остановить стрим нужен дополнительный код -> конец Streaming_4
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
        println(s"Stopped $description")
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
  def createParquetSink(df: DataFrame, fileName: String): DataStreamWriter[Row] =
    df
      .writeStream
      .queryName("rate-parquet")
      .format("parquet")
      .option("path", s"src/main/resources/l_7/$fileName")
      /**
      * без checkpointLocation - err:
      * AnalysisException: checkpointLocation must be specified either through option("checkpointLocation", ...) or SparkSession.conf.set("spark.sql.streaming.checkpointLocation", ...).
      */
      .option("checkpointLocation", s"src/main/resources/l_7/chk/$fileName")
      .trigger(Trigger.ProcessingTime("10 seconds"))

  val parquetSink: DataStreamWriter[Row] = createParquetSink(rateStreamDf, "s1.parquet")
//  val parquetSink: DataStreamWriter[Row] = createParquetSink(rateStreamDf.repartition(1), "s1.parquet")

//  val streamingQuery2: StreamingQuery = parquetSink.start()
//  streamingQuery2.awaitTermination(15000)
//  println(s"streamingQuery2.isActive: ${streamingQuery2.isActive}")
//  println(s"streamingQuery2.name: ${streamingQuery2.name}")
//  println()

//  println(s"streamingQuery2.lastProgress: \n${streamingQuery2.lastProgress}")
  /*
    {
      "id" : "058a0650-fe2f-4d74-a0ed-abefd4e8f844",
      "runId" : "655f1c6c-edcd-4e34-aaa8-dc2703f4e19f",
      "name" : "rate-parquet",
      "timestamp" : "2023-11-12T13:52:50.001Z",
      "batchId" : 6,
      "numInputRows" : 9,
      "inputRowsPerSecond" : 0.9570395576350489,
      "processedRowsPerSecond" : 42.25352112676057,
      "durationMs" : {
        "addBatch" : 140,
        "commitOffsets" : 18,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 10,
        "triggerExecution" : 213,
        "walCommit" : 42
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 85,
        "endOffset" : 94,
        "latestOffset" : 94,
        "numInputRows" : 9,
        "inputRowsPerSecond" : 0.9570395576350489,
        "processedRowsPerSecond" : 42.25352112676057
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
      "id" : "058a0650-fe2f-4d74-a0ed-abefd4e8f844",
      "runId" : "655f1c6c-edcd-4e34-aaa8-dc2703f4e19f",
      "name" : "rate-parquet",
      "timestamp" : "2023-11-12T13:52:38.624Z",
      "batchId" : 4,
      "numInputRows" : 59,
      "inputRowsPerSecond" : 0.0,
      "processedRowsPerSecond" : 30.132788559754854,
      "durationMs" : {
        "addBatch" : 1754,
        "commitOffsets" : 21,
        "getBatch" : 3,
        "latestOffset" : 0,
        "queryPlanning" : 40,
        "triggerExecution" : 1958,
        "walCommit" : 57
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 24,
        "endOffset" : 83,
        "latestOffset" : 83,
        "numInputRows" : 59,
        "inputRowsPerSecond" : 0.0,
        "processedRowsPerSecond" : 30.132788559754854
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    }, {
      "id" : "058a0650-fe2f-4d74-a0ed-abefd4e8f844",
      "runId" : "655f1c6c-edcd-4e34-aaa8-dc2703f4e19f",
      "name" : "rate-parquet",
      "timestamp" : "2023-11-12T13:52:40.597Z",
      "batchId" : 5,
      "numInputRows" : 2,
      "inputRowsPerSecond" : 1.0136847440446022,
      "processedRowsPerSecond" : 9.852216748768472,
      "durationMs" : {
        "addBatch" : 160,
        "commitOffsets" : 16,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 8,
        "triggerExecution" : 203,
        "walCommit" : 15
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 83,
        "endOffset" : 85,
        "latestOffset" : 85,
        "numInputRows" : 2,
        "inputRowsPerSecond" : 1.0136847440446022,
        "processedRowsPerSecond" : 9.852216748768472
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    }, {
      "id" : "058a0650-fe2f-4d74-a0ed-abefd4e8f844",
      "runId" : "655f1c6c-edcd-4e34-aaa8-dc2703f4e19f",
      "name" : "rate-parquet",
      "timestamp" : "2023-11-12T13:52:50.001Z",
      "batchId" : 6,
      "numInputRows" : 9,
      "inputRowsPerSecond" : 0.9570395576350489,
      "processedRowsPerSecond" : 42.25352112676057,
      "durationMs" : {
        "addBatch" : 140,
        "commitOffsets" : 18,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 10,
        "triggerExecution" : 213,
        "walCommit" : 42
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 85,
        "endOffset" : 94,
        "latestOffset" : 94,
        "numInputRows" : 9,
        "inputRowsPerSecond" : 0.9570395576350489,
        "processedRowsPerSecond" : 42.25352112676057
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    })
   */
  println()

//  println(s"streamingQuery2.status: \n${streamingQuery2.status}")
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

//  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")
//
//  val airportsDf: DataFrame =
//    spark
//      .read
//      .options(csvOptions)
//      .csv("src/main/resources/l_3/airport-codes.csv")
//
//  airportsDf.printSchema()
//  airportsDf.show(numRows = 1, truncate = 100, vertical = true)
//  println()
//
//  val idents: Array[String] =
//    airportsDf
//      .select($"ident")
//      .limit(200)
//      .distinct()
//      .as[String]
//      .collect()
//
//  println(s"idents: ${idents.mkString("Array(", ", ", ")")}")
//  println()

  /**
   * shuffle - перемешивает массив
   * shuffle(array)(0) - берет первый элемент случайно перемешанного массива
    */
//  val identStreamDf: DataFrame = rateStreamDf.withColumn("ident", shuffle(array(idents.map(lit): _*))(0))
//
//  val identParquetSink: DataStreamWriter[Row] = createParquetSink(identStreamDf, "s2.parquet")
//  val identStreamQuery: StreamingQuery = identParquetSink.start()
//  identStreamQuery.awaitTermination(15000)

//  val identParquetDf: DataFrame =
//    spark
//      .read
//      .parquet("src/main/resources/l_7/s2.parquet")
//
//  println(s"identParquetDf.count(): ${identParquetDf.count()}")
//  println()
//
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
  println()

//  identParquetDf.show(truncate = false)


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
