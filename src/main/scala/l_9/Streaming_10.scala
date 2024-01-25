package l_9

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.parallel.CollectionConverters._

import java.lang

object Streaming_10 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_9")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")
  println(sc.uiWebUrl)
  println(sc.master)
  println(sc.getSchedulingMode)
  println()

//  val df: Dataset[lang.Long] = spark.range(0, 100)
//  println(df.count())
//  df.show() // начнет выполняться только после окончания count
//  println(df.collect().mkString("Array(", ", ", ")"))  // начнет выполняться только после окончания show
//  println()

  val delay: UserDefinedFunction = udf { () => Thread.sleep(1000); true }

  def performanceTest(df: Dataset[java.lang.Long]): Array[Row] =
    spark.time {
      df
        .withColumn("foo", delay())
        .collect()
    }

  /** 1 партиция, 30 элементов */
  val testDf1: Dataset[lang.Long] = spark.range(0, 30, 1, 1)
  /** 30151 ms */
//  val res1: Array[Row] = performanceTest(testDf1)
//  println()

  /** 3 партиции, 30 элементов => 10 элементов в партиции */
  val testDf2: Dataset[lang.Long] = spark.range(0, 30, 1, 3)
  /** 10074 ms */
//  val res2: Array[Row] = performanceTest(testDf2)
//  println()

  /** 6 партиций, 30 элементов => 5 элементов в партиции */
  val testDf3: Dataset[lang.Long] = spark.range(0, 30, 1, 6)
  /** 5193 ms */
//  val res: Array[Row] = performanceTest(testDf3)
//  println()

  /** 10 партиций, 30 элементов => 3 элемента в партиции, но т.к. ядер 8 => 8 партиций (3с) + 2 партиции (3с)  */
  val testDf4: Dataset[lang.Long] = spark.range(0, 30, 1, 10)
  /** 7504 ms (в идеале должно быть 6с) ms */
//  val res4: Array[Row] = performanceTest(testDf4)
//  println()


  /** Последовательная обработка */
  /** 42332 ms (10 раз * 4 элемента в партиции => 40с) */
//  spark.time {
//    (1 to 10).foreach { _ =>
//      val testDf: Dataset[lang.Long] = spark.range(0, 16, 1, 4)

      /** !!! 8 ядер, но 4 партиции => 4 ядра простаивают */
//      testDf
//        .withColumn("foo", delay())
//        .collect()
//    }
//  }

  /** Параллельная обработка */
  /**
   * !!! 21655 ms - в 2 раза быстрее, т.к. задействованы 4 простаивающих ядра => в единицу времени обрабатываются 8 партиций
   * (5 раз * 4 элемента в партиции => 20с)
   */
  spark.time {
    (1 to 10)
      .par
      .foreach { _ =>
        val testDf: Dataset[lang.Long] = spark.range(0, 16, 1, 4)

        testDf
          .withColumn("foo", delay())
          .collect()
      }
  }


  Thread.sleep(1000000)

  spark.stop()
}

/** FAIR Scheduler */
object Streaming_11 extends App {
    // не работает в Spark 3.4.0
//  Logger
//    .getLogger("org")
//    .setLevel(Level.ERROR)

  val sparkConf: SparkConf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("l_9")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.scheduler.allocation.file", "src/main/resources/fairscheduler.xml")

//  conf.set("spark.scheduler.mode", "FAIR")
//  val sc: SparkContext = new SparkContext(conf)

  val spark: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")
  println(sc.uiWebUrl)
  println(sc.master)
  println(sc.getSchedulingMode)
  println(spark.conf.get("spark.scheduler.mode"))
  println()

  val delay: UserDefinedFunction = udf { () => Thread.sleep(1000); true }

  /**
   * 22267 ms
   *
   * В идеале должно быть 20с =>
   * 16с (8 джоб по 1 ядру на каждую => 4с на 1 партицию => 16c на 4 партиции)
   * + 4с (2 джобы по 4 ядра на каждую => 4c на 1 партицию => 4c на 4 партиции)
   */
  spark.time {
    (1 to 10)
      .par
      .foreach { _ =>
        val testDf: Dataset[lang.Long] = spark.range(0, 16, 1, 4)

        testDf
          .withColumn("foo", delay())
          .collect()
      }
  }


  Thread.sleep(1000000)

  spark.stop()
}
