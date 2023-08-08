package l_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{array, input_file_name, lit, log, shuffle}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object Streaming_1 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.OFF)

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
  def createConsoleSink(df: DataFrame): DataStreamWriter[Row] =
    df
      .writeStream
      .format("console")
      /** trigger - задает периодичность чтения/обработки новых данных */
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("truncate", "false")
      .option("numRows", "20")

  val rateStreamDf: DataFrame =
    spark
      .readStream
      .format("rate")
      .load()

  println(s"rateStreamDf.isStreaming: ${rateStreamDf.isStreaming}")
  println()

  /** err - Queries with streaming sources must be executed with writeStream.start() */
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
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@42c9b1ee, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@306cfb8a, [], [timestamp#0, value#1L]

    == Analyzed Logical Plan ==
    timestamp: timestamp, value: bigint
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@42c9b1ee, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@306cfb8a, [], [timestamp#0, value#1L]

    == Optimized Logical Plan ==
    StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider@42c9b1ee, rate, org.apache.spark.sql.execution.streaming.sources.RateStreamTable@306cfb8a, [], [timestamp#0, value#1L]

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
//    println("stop streaming")
//    /**
//     * останавливаем стрим
//     * stop - позволяет остановить (возобновить - нельзя), т.е. фактически "убить" стрим
//     */
//    streamingQuery.stop()
//
//
//    println("continue streaming")
//
//    /** продолжаем блокировать поток */
//    streamingQuery.awaitTermination()
//  }

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

//  while (true) {
//    Try {
//      streamingQuery.awaitTermination(5000)
//    } match {
//      case Failure(ex) =>
//        val sq = consoleSink.start()
//        sq.awaitTermination(5000)
//
//      case Success(value) =>
//    }
//  }


  /**
   * остановка всех стримов
   * !!! это жесткая остановка - если попасть в момент записи батча (например в БД) - часть данных записана не будет
   *
   * чтобы погасить стрим gracefully - надо писать код - конец Streaming_4
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
    // поток не блокируется
    System.exit(0)

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
      /** без checkpointLocation - err: AnalysisException: checkpointLocation must be specified */
      .option("checkpointLocation", s"src/main/resources/l_7/chk/$fileName")
      .trigger(Trigger.ProcessingTime("10 seconds"))

//  val parquetSink: DataStreamWriter[Row] = createParquetSink(rateStreamDf, "s1.parquet")
  val parquetSink: DataStreamWriter[Row] = createParquetSink(rateStreamDf.repartition(1), "s1.parquet")
//  val streamingQuery2: StreamingQuery = parquetSink.start()
//  streamingQuery2.awaitTermination(15000)
//  println(s"streamingQuery2.isActive: ${streamingQuery2.isActive}")
//  println(s"streamingQuery2.name: ${streamingQuery2.name}")
  println()

//  println(s"streamingQuery2.lastProgress: \n${streamingQuery2.lastProgress}")
  /*
    {
      "id" : "fbaf8cb8-b5f4-414e-9bd2-c046d9045b2a",
      "runId" : "06e66a01-d015-4eba-ad48-24f3445dd2e0",
      "name" : "rate-parquet",
      "timestamp" : "2023-08-05T11:00:20.000Z",
      "batchId" : 7,
      "numInputRows" : 7,
      "inputRowsPerSecond" : 1.0117068940598353,
      "processedRowsPerSecond" : 27.237354085603112,
      "durationMs" : {
        "addBatch" : 163,
        "commitOffsets" : 35,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 7,
        "triggerExecution" : 257,
        "walCommit" : 47
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 185,
        "endOffset" : 192,
        "latestOffset" : 192,
        "numInputRows" : 7,
        "inputRowsPerSecond" : 1.0117068940598353,
        "processedRowsPerSecond" : 27.237354085603112
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
      "id" : "fbaf8cb8-b5f4-414e-9bd2-c046d9045b2a",
      "runId" : "06e66a01-d015-4eba-ad48-24f3445dd2e0",
      "name" : "rate-parquet",
      "timestamp" : "2023-08-05T11:00:13.081Z",
      "batchId" : 6,
      "numInputRows" : 63,
      "inputRowsPerSecond" : 0.0,
      "processedRowsPerSecond" : 42.33870967741935,
      "durationMs" : {
        "addBatch" : 1331,
        "commitOffsets" : 29,
        "getBatch" : 2,
        "latestOffset" : 0,
        "queryPlanning" : 29,
        "triggerExecution" : 1484,
        "walCommit" : 27
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 122,
        "endOffset" : 185,
        "latestOffset" : 185,
        "numInputRows" : 63,
        "inputRowsPerSecond" : 0.0,
        "processedRowsPerSecond" : 42.33870967741935
      } ],
      "sink" : {
        "description" : "FileSink[src/main/resources/l_7/s1.parquet]",
        "numOutputRows" : -1
      }
    }, {
      "id" : "fbaf8cb8-b5f4-414e-9bd2-c046d9045b2a",
      "runId" : "06e66a01-d015-4eba-ad48-24f3445dd2e0",
      "name" : "rate-parquet",
      "timestamp" : "2023-08-05T11:00:20.000Z",
      "batchId" : 7,
      "numInputRows" : 7,
      "inputRowsPerSecond" : 1.0117068940598353,
      "processedRowsPerSecond" : 27.237354085603112,
      "durationMs" : {
        "addBatch" : 163,
        "commitOffsets" : 35,
        "getBatch" : 0,
        "latestOffset" : 0,
        "queryPlanning" : 7,
        "triggerExecution" : 257,
        "walCommit" : 47
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
        "startOffset" : 185,
        "endOffset" : 192,
        "latestOffset" : 192,
        "numInputRows" : 7,
        "inputRowsPerSecond" : 1.0117068940598353,
        "processedRowsPerSecond" : 27.237354085603112
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

  val parquetStreamDf: DataFrame = spark.read.load("src/main/resources/l_7/s1.parquet")
  println(parquetStreamDf.count())
  println()

  parquetStreamDf.printSchema()
  parquetStreamDf.show(5, truncate = false)

  /** input_file_name() - позволяет просмотреть файлы, являющиеся источниками датафрейма */
  val uniqFilesDs: Dataset[Row] =
    parquetStreamDf
      .select(input_file_name())
      .distinct()

  uniqFilesDs.show(20, truncate = false)
  println(uniqFilesDs.count())
  println()

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
  val identStreamQuery: StreamingQuery = identParquetSink.start()
  identStreamQuery.awaitTermination(15000)

  val identParquetDf: DataFrame =
    spark
      .read
      .parquet("src/main/resources/l_7/s2.parquet")

  println(s"identParquetDf.count(): ${identParquetDf.count()}")
  println()

  identParquetDf.printSchema()
  println(identParquetDf.schema.toDDL)
  println(identParquetDf.schema.json)
  println()

  identParquetDf.show(truncate = false)


  Thread.sleep(1000000)

  spark.stop()
}
