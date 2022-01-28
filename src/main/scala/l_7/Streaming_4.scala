package l_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{from_json, spark_partition_id}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Streaming_4 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_7")
    .getOrCreate

  import spark.implicits._

  def createConsoleSink(df: DataFrame): DataStreamWriter[Row] =
    df
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("truncate", "false")
      .option("numRows", "20")

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

  val kafkaParams: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> "localhost:9092",
    "subscribe" -> "test_topic0",
    "startingOffsets" -> """earliest""",
    /** !!! Лимит вычитывания сообщений - по всем партициям. */
    "maxOffsetsPerTrigger" -> "5",
    /** !!! Позволяет дробить топики на партиции - воркеры буду читать партицию в несколько потоков (по разным диапазонам оффсетов). */
    "minPartitions"-> "20"
  )

  val streamingDf: DataFrame = spark
    .readStream
    .format("kafka")
    .options(kafkaParams)
    .load()

  val schema: DataType =
    DataType.fromJson("""{"type":"struct","fields":[{"name":"timestamp","type":"timestamp","nullable":true,"metadata":{}},{"name":"value","type":"long","nullable":true,"metadata":{}},{"name":"ident","type":"string","nullable":true,"metadata":{}}]}""")

  val parsedStreamingDf: DataFrame = streamingDf
    .select(
      'value.cast("string"),
      'topic,
      'partition,
      'offset
    )
    .withColumn("value", from_json('value, schema))
    .select('topic, 'partition, 'offset, $"value.*")
    .withColumn("partition_id", spark_partition_id())

  /** Если батч пустой - запустить Streaming_3 => writeKafka("test_topic0", identParquetDf) */
  val sink: DataStreamWriter[Row] = createConsoleSink(parsedStreamingDf)
//  val streamingQuery: StreamingQuery = sink.start()

  def createConsoleSinkWithCheckpoint(chkName: String, df: DataFrame): DataStreamWriter[Row] =
    df
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      /** !!! Без опции checkpointLocation при каждом перезапуске стрима будут вычитываться все данные. */
      .option("checkpointLocation", s"src/main/resources/l_7/chk/$chkName")
      .option("truncate", "false")
      .option("numRows", "20")

  val sinkWithCheckpoint: DataStreamWriter[Row] = createConsoleSinkWithCheckpoint("s3.kafka", parsedStreamingDf)
//  val streamingQuery2: StreamingQuery = sinkWithCheckpoint.start()


  /** Graceful stream shutdown. */
  val testStreamingDf: DataFrame = spark
    .readStream
    .format("rate")
    .load()

  val testStreamingQuery: StreamingQuery = testStreamingDf
    .writeStream
    .format("console")
    .start()

//  while (testStreamingQuery.status.isTriggerActive) { // isTriggerActive будет false, как только будет батч будет обработан (произведена запись в sink).
//    ()
//  }
  /** После isTriggerActive == false останавливаем стрим. */
//  testStreamingQuery.stop()

  /** Пример с маркер-файлом. */
  val isStopFile: Boolean = true
  while (testStreamingQuery.status.isTriggerActive && !isStopFile) {
    testStreamingQuery.awaitTermination(10000)
  }
  testStreamingQuery.stop()

  /** Еще более безопасно можно останавливать стрим с помощью foreachBatch. */

  Thread.sleep(1000000)
}
