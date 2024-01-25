package l_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{from_json, spark_partition_id}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{DataType, DataTypes, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Streaming_4 extends App {
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

  def createConsoleSink(df: DataFrame): DataStreamWriter[Row] = {
    df
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("truncate", "false")
      .option("numRows", "20")
  }

  /**
   * Запуск в докере:
   * docker run --rm -p 2181:2181 --name=test_zoo -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper
   * docker inspect test_zoo --format='{{ .NetworkSettings.IPAddress }}'
   *
   * docker run --rm -p 9092:9092 --name=test_kafka -e KAFKA_ZOOKEEPER_CONNECT=172.17.0.2:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka
   *
   * docker ps
   * docker stop
   */

  val kafkaParams: Map[String, String] =
    Map(
      "kafka.bootstrap.servers" -> "localhost:9092",
      "subscribe" -> "test_topic0",
      "startingOffsets" -> "earliest",
      /** !!! maxOffsetsPerTrigger - лимит вычитывания сообщений - по всем партициям */
      "maxOffsetsPerTrigger" -> "5",
      /**
       * !!! minPartitions - позволяет дробить партиции на более мелкие части
       * воркеры буду читать партицию в несколько потоков (по разным диапазонам оффсетов)
       */
      "minPartitions" -> "20"
    )

  val streamingDf: DataFrame =
    spark
      .readStream
      .format("kafka")
      .options(kafkaParams)
      .load()

  val schema: DataType =
    DataType.fromJson("""{"type":"struct","fields":[{"name":"timestamp","type":"timestamp","nullable":true,"metadata":{}},{"name":"value","type":"long","nullable":true,"metadata":{}},{"name":"ident","type":"string","nullable":true,"metadata":{}}]}""")

  val parsedStreamingDf: DataFrame =
    streamingDf
      /**
       * без .cast(StringType) - err:
       * AnalysisException: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "from_json(value)"
       * due to data type mismatch: Parameter 1 requires the "STRING" type, however "value" has the type "BINARY"
       */
//      .withColumn("value", from_json($"value", schema))
      .withColumn("value", from_json($"value".cast(StringType), schema))
      .select(
        $"topic",
        $"partition",
        $"offset",
        $"value.*",
        spark_partition_id().as("partition_id")
      )

  parsedStreamingDf.explain()
  /*
    == Physical Plan ==
    *(1) Project [topic#9, partition#10, offset#11L, value#21.timestamp AS timestamp#40, value#21.value AS value#41L, value#21.ident AS ident#42, SPARK_PARTITION_ID() AS partition_id#29]
    +- Project [from_json(StructField(timestamp,TimestampType,true), StructField(value,LongType,true), StructField(ident,StringType,true), cast(value#8 as string), Some(Europe/Moscow)) AS value#21, topic#9, partition#10, offset#11L]
       +- StreamingRelation kafka, [key#7, value#8, topic#9, partition#10, offset#11L, timestamp#12, timestampType#13]
   */

  /** Если батч пустой - запустить Streaming_3 => writeKafka("test_topic0", identParquetDf) */
  val sink: DataStreamWriter[Row] = createConsoleSink(parsedStreamingDf)
//  val streamingQuery: StreamingQuery = sink.start()

  def createConsoleSinkWithCheckpoint(chkName: String, df: DataFrame): DataStreamWriter[Row] = {
    df
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      /** !!! без checkpointLocation - при перезапуске стрима топик будет вычитываться с начала */
      .option("checkpointLocation", s"src/main/resources/l_7/chk/$chkName")
      .option("truncate", "false")
      .option("numRows", "20")
  }

  val sinkWithCheckpoint: DataStreamWriter[Row] = createConsoleSinkWithCheckpoint("s3.kafka", parsedStreamingDf)
//  val streamingQuery2: StreamingQuery = sinkWithCheckpoint.start()


  /** Graceful stream shutdown */
  val testStreamingDf: DataFrame =
    spark
      .readStream
      .format("rate")
      .load()

  val gracefulSink: DataStreamWriter[Row] =
    testStreamingDf
      .writeStream
      .format("console")

  val streamingQuery: StreamingQuery = gracefulSink.start()
  Thread.sleep(5000)

  /** isTriggerActive будет false, как только батч будет полностью обработан (т.е. произведена запись в sink) */
//  while (streamingQuery.status.isTriggerActive) {
//    println("processing is active")
//  }
//  println("waiting for next trigger")
  /** как только isTriggerActive станет false - останавливаем стрим */
//  streamingQuery.stop()

  /** Пример с маркер-файлом */
  val isStopFile: Boolean = true
  while (streamingQuery.status.isTriggerActive || !isStopFile) {
    streamingQuery.awaitTermination(5000)
  }
  streamingQuery.stop()

  /** Более безопасно можно останавливать стрим с помощью foreachBatch - описание алгоритма с 2-50-00 */


  Thread.sleep(1000000)

  spark.stop()
}
