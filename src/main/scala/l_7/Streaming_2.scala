package l_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Streaming_2 extends App {
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

  def createConsoleSink(df: DataFrame): DataStreamWriter[Row] =
    df
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("truncate", "false")
      .option("numRows", "20")

  val streamingFromParquetDf: DataFrame =
    spark
      .readStream
      .format("parquet")
    /**
     * Поскольку в директорию могут попасть любые данные, а df должен иметь фиксированную схему,
     * то Spark не позволяет нам создавать streamingDf на основе файлов без указания схемы
     *
     * без - err: java.lang.IllegalArgumentException - Schema must be specified when creating a streaming source DataFrame
     */
    // v1
    .schema(StructType.fromDDL("timestamp TIMESTAMP,value BIGINT,ident STRING"))
    // v2
//    .schema(DataType.fromJson(someJson)))
    /** сколько файлов будет вычитано за микробатч */
    .option("maxFilesPerTrigger", "1")
    /** можно указать вложенные директории - с помощью "*" */
    .option("path", "src/main/resources/l_7/s2.parquet")
    .load()
    // можно применять любые трансформации
    .withColumn("ident", lower($"ident"))

  streamingFromParquetDf.printSchema()

  val consoleSink: DataStreamWriter[Row] = createConsoleSink(streamingFromParquetDf)
  consoleSink.start()


  Thread.sleep(1000000)

  spark.stop()
}
