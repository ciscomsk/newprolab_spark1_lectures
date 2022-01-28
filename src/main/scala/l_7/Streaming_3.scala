package l_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{from_json, lit, struct, to_json}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Streaming_3 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_7")
    .getOrCreate

  import spark.implicits._

  /**
   * https://github.com/confluentinc/cp-docker-images/issues/801
   *
   * Запуск в докере:
   * docker run --rm -p 2181:2181 --name=test_zoo -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper
   * docker inspect test_zoo --format='{{ .NetworkSettings.IPAddress }}'
   *
   * ++ docker run --rm -p 9092:9092 --name=test_kafka -e KAFKA_ZOOKEEPER_CONNECT=172.17.0.2:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka
   * -- docker run --rm -p 9092:9092 --name=test_kafka -e KAFKA_ZOOKEEPER_CONNECT=host.docker.internal:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://host.docker.internal:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka
   *
   * docker ps
   * docker stop
   */

  val identParquetDf: DataFrame = spark
    .read
    .parquet("src/main/resources/l_7/s2.parquet")

  println(identParquetDf.count)
  println(identParquetDf.schema.json)
  println()

  identParquetDf.printSchema()
  identParquetDf.show(truncate = false)


  def writeToKafka[T](topic: String, data: Dataset[T]): Unit = {
    val kafkaParams: Map[String, String] = Map(
      "kafka.bootstrap.servers" -> "localhost:9092"
    )

    data
      /** v1 - toJSON - дорогая операция. */
//      .toJSON
      /** v2 - to_json - более производительная операция. */
      .select(to_json(struct("*")).alias("value"))
      // без - err - AnalysisException: topic option required when no 'topic' attribute is present. Use the topic option for setting a topic
      .withColumn("topic", lit(topic))
      .write
      .format("kafka")
      .options(kafkaParams)
      .save
  }

  /**
   * Если топик с данным именем не существует - он будет создан c дефолтными настройками.
   * !!! На проде топики нужно создавать вручную с нужными настройками.
   */
//  writeToKafka("test_topic0", identParquetDf)

  val kafkaParams: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> "localhost:9092",
    "subscribe" -> "test_topic0"
  )

  /** Чтение из Kafka в СТАТИЧЕСКИЕ датафреймы. */
  /** Поля timestamp/timestampType на данный момент не используются в Kafka. */
  val kafkaStaticDf: DataFrame = spark
    .read
    .format("kafka")
    .options(kafkaParams)
    .load()

  kafkaStaticDf.printSchema()
  kafkaStaticDf.show()

  /** Парсинг данных из Kafka. */
  /** v1 - дорого и не подходит для стримов. */
  val jsonDs: Dataset[String] = kafkaStaticDf
    .select('value.cast(StringType))
    .as[String]

  jsonDs.printSchema()
  jsonDs.show(truncate = false)

  val parsedDf: DataFrame = spark
    .read
    .json(jsonDs)

  parsedDf.printSchema()
  parsedDf.show(truncate = false)

  /** v2 - более производительное решение. */
  val jsonDf2: DataFrame = kafkaStaticDf.select('value.cast("string").alias("value"))
  val schema: StructType = identParquetDf.schema

  val parsedDf2: DataFrame = jsonDf2.select(from_json('value, schema).alias("value"))
  parsedDf2.printSchema()
  /** !!! 1 - т.к. топик по дефолту создается с 1-й партицией. */
  println(parsedDf2.rdd.getNumPartitions)

  parsedDf2
    .select($"value.*")
    .show(truncate = false)

  /** Вычитка определенной части топика. */
  /** Возьмем 2 случайных оффсета. */
  kafkaStaticDf
    .sample(0.1)
    .limit(2)
    .select('topic, 'partition, 'offset)
    .show()

  val kafkaParamsWithOffsets: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> "localhost:9092",
    "subscribe" -> "test_topic0",
    /** Если читается больше 1 топика - оффсеты нужно указать для всех. */
    "startingOffsets" -> """ { "test_topic0": { "0": 9 } } """,
    /** Не включая 18 оффсет. */
    "endingOffsets" -> """ { "test_topic0": { "0": 18 } } """
  )

  val kafkaStaticWithOffsetsDf: DataFrame = spark
    .read
    .format("kafka")
    .options(kafkaParams)
    .load()

  kafkaStaticDf.printSchema()
  kafkaStaticDf.show(truncate = false)

  Thread.sleep(1000000)
}
