package l_9

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, lit, shuffle, split, udf}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}

// Scala 2.13
import scala.collection.parallel.CollectionConverters._

object Streaming_9 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_9")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  case class Category(name: String, count: Long)

  def airportsDf() = {
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")
  }

  def getRandomIdent(): Column = {
    val idents: Array[String] = airportsDf()
      .select('ident)
      .limit(20)
      .distinct
      .as[String]
      .collect()

    val columnArray: Array[Column] = idents.map(lit)
    val sparkArray: Column = array(columnArray: _*)
    val shuffledArray: Column = shuffle(sparkArray)

    shuffledArray(0)
  }

  def createSink(chkName: String, df: DataFrame)(batchFunc: (DataFrame, Long) => Unit): DataStreamWriter[Row] =
    df
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_9/chk/$chkName")
      .foreachBatch(batchFunc)

  val myStreamDf: DataFrame = spark
    .readStream
    .format("rate")
    .load()
    .withColumn("ident", getRandomIdent())

  println(sc.uiWebUrl)

  val udf_wait: UserDefinedFunction = udf { () => Thread.sleep(1000); true }

  createSink("state7", myStreamDf) { (df, id) =>
    df.cache()
    val count: Long = df.count()
    val schema: StructType = df.schema

    println(schema.simpleString)
    println(s"Count: $count")
    println(s"BatchId: $id")
    println()

    val withSymbolDf: DataFrame = df.withColumn("name", split('ident, "")(2))

    withSymbolDf.cache()
    withSymbolDf.count()

    val categories: Array[Category] = withSymbolDf
      .groupBy('name)
      .count
      .as[Category]
      .collect()

    /** Объединим датафрейм в 1 партицию - чтобы в categories.foreach записывалось по 1 файлу для каждой категории - снижаем нагрузку на hdfs. */
    val coalescedDf: Dataset[Row] = withSymbolDf.coalesce(1)

    categories.foreach { category =>
      val cName: String = category.name

      val filteredDf: Dataset[Row] = coalescedDf.filter('name === cName)
    /**
     * !!! Запись выполняется в 1 ПОТОК (т.к. coalesce(1)) и является БЛОКИРУЮЩЕЙ операцией - т.е. остальные ядра в этот момент простаивают.
     * Spark UI => Executors => Cores/Active Tasks.
     */
      filteredDf
        /** Искусственное замедление - так проблема лучше видна в Spark UI. */
        .withColumn("wait", udf_wait())
        .write
        .mode(SaveMode.Append)
        .parquet(s"src/main/resources/l_9/state7.parquet/$cName")
    }

    withSymbolDf.unpersist()
    df.unpersist()
  }
//    .start()

  /** Вариант с параллельной коллекцией. */
  createSink("state8", myStreamDf) { (df, id) =>
    df.cache()
    val count: Long = df.count()
    val schema: StructType = df.schema

    println(schema.simpleString)
    println(s"Count: $count")
    println(s"BatchId: $id")
    println()

    val withSymbolDf: DataFrame = df.withColumn("name", split('ident, "")(2))

    withSymbolDf.cache()
    withSymbolDf.count()

    val categories: Array[Category] = withSymbolDf
      .groupBy('name)
      .count()
      .as[Category]
      .collect()

    val coalescedDf: Dataset[Row] = withSymbolDf.coalesce(1)

    /**
     *  Parallel collection - операции над ними происходят асинхронно.
     *  Каждый foreach будет работать в своем потоке.
     */
    categories.par.foreach { category =>
      val cName: String = category.name

      val filteredDf: Dataset[Row] = coalescedDf.filter('name === cName)

      filteredDf
        .withColumn("wait", udf_wait())
        .write
        .mode(SaveMode.Append)
        .parquet(s"src/main/resources/l_9/state8.parquet/$cName")
    }

    withSymbolDf.unpersist()
    df.unpersist()
  }
//    .start()

  Thread.sleep(1000000)
}

object ParallelCollections extends App {
  (0 to 10).foreach(println)
  println()

  (0 to 10).par.foreach(println)
}
