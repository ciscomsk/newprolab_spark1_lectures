package l_9

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, lit, shuffle, split, udf}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.collection.parallel.CollectionConverters._

object Streaming_9 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_9")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")
  println(sc.uiWebUrl)
  println()

  import spark.implicits._

  case class Category(name: String, count: Long)

  def airportsDf(): DataFrame = {
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")
  }

  def getRandomIdent: Column = {
    val idents: Array[String] =
      airportsDf()
        .select($"ident")
        .limit(20)
        .distinct()
        .as[String]
        .collect()

    val columnArray: Array[Column] = idents.map(lit)
    val sparkArray: Column = array(columnArray: _*)
    val shuffledArray: Column = shuffle(sparkArray)

    shuffledArray(0)
  }

  def createSink(chkName: String, df: DataFrame)(batchFunc: (DataFrame, Long) => Unit): DataStreamWriter[Row] = {
    df
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_9/chk/$chkName")
      .foreachBatch(batchFunc)
  }

  val myStreamDf: DataFrame =
    spark
      .readStream
      .format("rate")
      .load()
      .withColumn("ident", getRandomIdent)


  val udf_wait: UserDefinedFunction = udf { () => Thread.sleep(1000); true }

  createSink("state7", myStreamDf) { (df, id) =>
    df.cache()
    /** count - 8 (partial) + 1 task */
    val count: Long = df.count()
    val schema: StructType = df.schema

    println(schema.simpleString)
    println(s"Count: $count")
    println(s"BatchId: $id")
    println()
//    df.show()

    val withSymbolDf: DataFrame = df.withColumn("name", split($"ident", "")(2))

//    withSymbolDf.explain()
    /*
      == Physical Plan ==
      *(1) Project [timestamp#0, value#1L, ident#51, split(ident#51, , -1)[2] AS name#166]
      +- InMemoryTableScan [ident#51, timestamp#0, value#1L]
            +- InMemoryRelation [timestamp#0, value#1L, ident#51], StorageLevel(disk, memory, deserialized, 1 replicas)
                  +- *(1) Scan ExistingRDD[timestamp#0,value#1L,ident#51]
     */

    withSymbolDf.cache()
    /** count - 8 (partial) + 1 task */
    withSymbolDf.count()
    df.unpersist()

    val categoriesDs: Dataset[Category] =
      withSymbolDf
        /** groupBy - 8 (partial) + 200 task */
        .groupBy($"name")
        .count()
        .as[Category]

//    categoriesDs.explain()
    /*
      == Physical Plan ==
      *(2) HashAggregate(keys=[name#166], functions=[count(1)])
      +- Exchange hashpartitioning(name#166, 200), ENSURE_REQUIREMENTS, [plan_id=164]
         +- *(1) HashAggregate(keys=[name#166], functions=[partial_count(1)])
            +- InMemoryTableScan [name#166]
                  +- InMemoryRelation [timestamp#0, value#1L, ident#51, name#166], StorageLevel(disk, memory, deserialized, 1 replicas)
                        +- *(1) Project [timestamp#0, value#1L, ident#51, split(ident#51, , -1)[2] AS name#166]
                           +- InMemoryTableScan [ident#51, timestamp#0, value#1L]
                                 +- InMemoryRelation [timestamp#0, value#1L, ident#51], StorageLevel(disk, memory, deserialized, 1 replicas)
                                       +- *(1) Scan ExistingRDD[timestamp#0,value#1L,ident#51]
     */

    val categories: Array[Category] = categoriesDs.collect()

    /**
     * объединяем датафрейм в 1 партицию
     * чтобы в categories.foreach записывалось по 1 файлу для каждой категории => снижаем нагрузку на hdfs
     */
    val coalescedDf: Dataset[Row] = withSymbolDf.coalesce(1)

//    coalescedDf.explain()
    /*
      == Physical Plan ==
      Coalesce 1
      +- InMemoryTableScan [timestamp#0, value#1L, ident#51, name#166]
            +- InMemoryRelation [timestamp#0, value#1L, ident#51, name#166], StorageLevel(disk, memory, deserialized, 1 replicas)
                  +- *(1) Project [timestamp#0, value#1L, ident#51, split(ident#51, , -1)[2] AS name#166]
                     +- InMemoryTableScan [ident#51, timestamp#0, value#1L]
                           +- InMemoryRelation [timestamp#0, value#1L, ident#51], StorageLevel(disk, memory, deserialized, 1 replicas)
                                 +- *(1) Scan ExistingRDD[timestamp#0,value#1L,ident#51]
     */

    coalescedDf.cache()
    /** count - 1 task (т.к. coalesce == 1) */
    coalescedDf.count()
    withSymbolDf.unpersist()

    categories.foreach { category =>
      val cName: String = category.name
      val filteredDf: Dataset[Row] = coalescedDf.filter($"name" === cName)

      /** udf_wait - искусственное замедление, так проблема лучше видна в Spark UI */
      val resDf: DataFrame = filteredDf.withColumn("wait", udf_wait())

//      resDf.explain()
      /*
        == Physical Plan ==
        *(1) Project [timestamp#0, value#1L, ident#51, name#873, UDF() AS wait#1476]
        +- *(1) Filter (isnotnull(name#873) AND (name#873 = C))
           +- InMemoryTableScan [ident#51, name#873, timestamp#0, value#1L], [isnotnull(name#873), (name#873 = C)]
                 +- InMemoryRelation [timestamp#0, value#1L, ident#51, name#873], StorageLevel(disk, memory, deserialized, 1 replicas)
                       +- Coalesce 1
                          +- InMemoryTableScan [timestamp#0, value#1L, ident#51, name#873]
                                +- InMemoryRelation [timestamp#0, value#1L, ident#51, name#873], StorageLevel(disk, memory, deserialized, 1 replicas)
                                      +- *(1) Project [timestamp#0, value#1L, ident#51, split(ident#51, , -1)[2] AS name#873]
                                         +- InMemoryTableScan [ident#51, timestamp#0, value#1L]
                                               +- InMemoryRelation [timestamp#0, value#1L, ident#51], StorageLevel(disk, memory, deserialized, 1 replicas)
                                                     +- *(1) Scan ExistingRDD[timestamp#0,value#1L,ident#51]
       */

      /**
       * !!! запись выполняется последовательно в 1 ПОТОК (т.к. coalesce(1)) и является БЛОКИРУЮЩЕЙ операцией (остальные ядра в этот момент простаивают)
       * !!! пока не закончится запись определенный ident - запись следующего не начнется
       * Spark UI => Executors => Cores/Active Tasks
       */
      resDf
        .write
        .mode(SaveMode.Append)
        .parquet(s"src/main/resources/l_9/state7.parquet/$cName")
    }

    coalescedDf.unpersist()
  }
//    .start()

  /** v1 - параллельные коллекции */
  createSink("state8", myStreamDf) { (df, id) =>
    df.cache()
    val count: Long = df.count()
    val schema: StructType = df.schema

    println(schema.simpleString)
    println(s"Count: $count")
    println(s"BatchId: $id")
    println()

    val withSymbolDf: DataFrame = df.withColumn("name", split($"ident", "")(2))
    withSymbolDf.cache()
    withSymbolDf.count()
    df.unpersist()

    val categories: Array[Category] =
      withSymbolDf
        .groupBy($"name")
        .count()
        .as[Category]
        .collect()

    val coalescedDf: Dataset[Row] = withSymbolDf.coalesce(1)
    coalescedDf.cache()
    coalescedDf.count()
    withSymbolDf.unpersist()

    /**
     *  Parallel collection - операции над элементами выполняются асинхронно
     *  каждый foreach будет работать в своем потоке
     */
    categories
      .par
      .foreach { category =>
        val cName: String = category.name
        val filteredDf: Dataset[Row] = coalescedDf.filter($"name" === cName)

        filteredDf
          .withColumn("wait", udf_wait())
          .write
          .mode(SaveMode.Append)
          .parquet(s"src/main/resources/l_9/state8.parquet/$cName")
      }

    coalescedDf.unpersist()
  }
//    .start()


  Thread.sleep(1000000)

  spark.stop()
}
