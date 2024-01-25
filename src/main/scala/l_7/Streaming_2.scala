package l_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Streaming_2 extends App {
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

  val streamingFromParquetDf: DataFrame =
    spark
      .readStream
      .format("parquet")
    /**
     * Поскольку в директорию могут попасть любые данные, а DF должен иметь фиксированную схему
     * -> Spark не даст создать streamingDf на основе файлов без указания схемы
     *
     * без schema - err:
     * SparkIllegalArgumentException: Schema must be specified when creating a streaming source DataFrame.
     * If some files already exist in the directory, then depending on the file format you may be able to create
     * a static DataFrame on that directory with 'spark.read.load(directory)' and infer schema from it
     */
    // v1
    .schema(StructType.fromDDL("timestamp TIMESTAMP,value BIGINT,ident STRING"))
    // v2
//    .schema(DataType.fromJson(someJson)))
    /** maxFilesPerTrigger - количество файлов, которое будет вычитано за микробатч */
    .option("maxFilesPerTrigger", "1")
    /** path - можно указать вложенные директории - с помощью "*" */
    .option("path", "src/main/resources/l_7/s2.parquet")
    .load()
    // можно применять любые трансформации
    .withColumn("ident", lower($"ident"))

  streamingFromParquetDf.printSchema()
  /*
    root
     |-- timestamp: timestamp (nullable = true)
     |-- value: long (nullable = true)
     |-- ident: string (nullable = true)
   */

  val consoleSink: DataStreamWriter[Row] = createConsoleSink(streamingFromParquetDf)
  consoleSink.start()


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
