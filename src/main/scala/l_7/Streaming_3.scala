package l_7

import org.apache.kafka.clients.admin.{AdminClient, TopicDescription}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.{TopicPartition, TopicPartitionInfo}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, from_json, lit, max, struct, to_json}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util
import java.util.Properties
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsJava}
import scala.util.Using

object Streaming_3 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

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

  /**
   * https://github.com/confluentinc/cp-docker-images/issues/801
   *
   * запуск в докере:
   * docker run --rm -p 2181:2181 --name=test_zoo -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper
   * docker inspect test_zoo --format='{{ .NetworkSettings.IPAddress }}'
   *
   * docker run --rm -p 9092:9092 --name=test_kafka -e KAFKA_ZOOKEEPER_CONNECT=172.17.0.2:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka
   *
   * docker ps
   * docker stop
   */

  val identParquetDf: DataFrame =
    spark
      .read
      .parquet("src/main/resources/l_7/s2.parquet")

  println(identParquetDf.count())
  println()
  println(identParquetDf.schema.json)
  println()

  identParquetDf.printSchema()
  identParquetDf.show(truncate = false)

  def writeToKafka[T](topic: String, data: Dataset[T]): Unit = {
    val kafkaParams: Map[String, String] = Map("kafka.bootstrap.servers" -> "localhost:9092")

    data
      /** v1 - toJSON - дорогая операция */
//      .toJSON
      /** v2 - to_json - более производительная операция */
      .select(to_json(struct("*")).alias("value"))
      // без topic - err: topic option required when no 'topic' attribute is present. Use the topic option for setting a topic.
      .withColumn("topic", lit(topic))
      .write
      .format("kafka")
      .options(kafkaParams)
      .save()
  }

  /**
   * если топик с данным именем не существует - он будет создан c дефолтными настройками
   * !!! на проде топики нужно создавать вручную с нужными настройками
   */
//  writeToKafka("test_topic0", identParquetDf)

  val kafkaParams: Map[String, String] =
    Map(
      "kafka.bootstrap.servers" -> "localhost:9092",
      "subscribe" -> "test_topic0"
    )

  /** чтение из Kafka в СТАТИЧЕСКИЙ датафрейм */

  /** поля timestamp/timestampType на данный момент не используются в Kafka */
  val kafkaStaticDf: DataFrame =
    spark
      .read
      .format("kafka")
      .options(kafkaParams)
      .load()

  println("kafkaStaticDf: ")
  kafkaStaticDf.printSchema()
  /*
    root
     |-- key: binary (nullable = true)
     |-- value: binary (nullable = true)
     |-- topic: string (nullable = true)
     |-- partition: integer (nullable = true)
     |-- offset: long (nullable = true)
     |-- timestamp: timestamp (nullable = true)
     |-- timestampType: integer (nullable = true)
   */
  kafkaStaticDf.cache()
  println(kafkaStaticDf.count())
  kafkaStaticDf.select(max(col("offset"))).show
  kafkaStaticDf.show()

  /** парсинг данных из Kafka */
  /** v1 - дорого + не подходит для стримов */
  val jsonDs: Dataset[String] =
    kafkaStaticDf
      .select($"value".cast(StringType))
      .as[String]

  jsonDs.printSchema()
  jsonDs.show(truncate = false)

  val parsedDf: DataFrame =
    spark
      .read
      .json(jsonDs)

  parsedDf.printSchema()
  parsedDf.show(truncate = false)

  /**
   * v2 - более производительное решение
   * .cast(StringType) - binary => StringType
   */
  val jsonDf2: DataFrame = kafkaStaticDf.select($"value".cast(StringType).alias("value"))
  val schema: StructType = identParquetDf.schema

  val parsedDf2: DataFrame = jsonDf2.select(from_json($"value", schema).alias("value"))
  println("parsedDf2: ")
  parsedDf2.printSchema()
  /** !!! 1 - т.к. топик по дефолту создается с 1-й партицией */
  println(s"parsedDf2.rdd.getNumPartitions: ${parsedDf2.rdd.getNumPartitions}")
  println()

  parsedDf2
    .select($"value.*")
    .show(truncate = false)

  /** вычитка определенной части топика */
  kafkaStaticDf
    .sample(0.1)
    .limit(2)
    .select($"topic", $"partition", $"offset")
    .show()

  val kafkaParamsWithOffsets: Map[String, String] =
    Map(
      "kafka.bootstrap.servers" -> "localhost:9092",
      "subscribe" -> "test_topic0",
      /** возьмем 2 случайных оффсета */
      /** если читается больше 1 топика - оффсеты нужно указать для всех */
      "startingOffsets" -> """ { "test_topic0": { "0": 200 } } """,
      /**
       * !!! не включая 207-й оффсет
       * https://kafka.apache.org/35/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
       * endOffsets - end offset is the high watermark ...
       *
       * если указать 208 - зависнет, пока не будет записано еще 1 сообщение
       *
       * если указать startingOffsets = 0 и endingOffsets = 0 - будет пустой датафрейм
       * если указать startingOffsets = 0 и endingOffsets = 1 - будет датафрейм с 1-й колонкой (с нулевым оффсетом)
       */
      "endingOffsets" -> """ { "test_topic0": { "0": 207 } } """
    )

  val kafkaStaticWithOffsetsDf: DataFrame =
    spark
      .read
      .format("kafka")
      .options(kafkaParamsWithOffsets)
      .load()

  println("kafkaStaticWithOffsetsDf: ")
  kafkaStaticWithOffsetsDf.printSchema()
  kafkaStaticWithOffsetsDf.show()


  val consumerProps: Properties = new Properties()
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "some_group")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val adminProps: Properties = new Properties()
  adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val adminClient: AdminClient = AdminClient.create(adminProps)
  val topic: String = "test_topic0"

  val topicDesc: TopicDescription =
    adminClient
      .describeTopics(List(topic).asJavaCollection)
      .topicNameValues()
      .get(topic)
      .get()

  val topicPartitions: util.Collection[TopicPartition] =
    topicDesc
      .partitions()
      .asScala
      .map(tpi => new TopicPartition(topicDesc.name(), tpi.partition()))
      .asJavaCollection

  Using(new KafkaConsumer[String, String](consumerProps)) { consumer =>
    println(s"endOffsets: ${consumer.endOffsets(topicPartitions)}") // endOffsets: {test_topic0-0=207}
  }


  Thread.sleep(1000000)

  spark.stop()
}
