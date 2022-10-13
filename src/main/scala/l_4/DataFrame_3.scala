package l_4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, lit, pmod, rand, round, sum, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, RelationalGroupedDataset, Row, SparkSession}

import java.lang

object DataFrame_3 extends App {
  // не работает в Spark 3.3.0
//  Logger
//    .getLogger("org")
//    .setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("DataFrame_3")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

//  spark.conf.set("spark.sql.adaptive.enabled", true)
//  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)
//  spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)
//  spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
//  spark.conf.getAll.foreach(println)
//  sc.getConf.getAll.foreach(println)

  import spark.implicits._


  /** Caching */
  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

  airportsDf.show(numRows = 1, truncate = 100, vertical = true)
  airportsDf.printSchema()

  val onlyRuAndHigh: Dataset[Row] = airportsDf.filter($"iso_country" === "RU" and $"elevation_ft" > 1000)

  /** !!! Несмотря на то, что onlyRuAndHigh является общим для всех действий ниже, весь граф по его расчету будет выполняться при вызове каждого действия */
  val start1: Long = System.currentTimeMillis()
  onlyRuAndHigh.count()
  onlyRuAndHigh.show(numRows = 1, truncate = 100, vertical = true)
  onlyRuAndHigh.collect()

  onlyRuAndHigh
    .groupBy($"municipality")
    .count()
    .orderBy($"count".desc)
    .na.drop("any")

  val end1: Long = System.currentTimeMillis()
  println(s"time1: ${(end1.toDouble - start1) / 1000}")  // 1.061s

  /**
   * !!! cache/persist - ленивые
   * => best practice - после cache/persist выполнять count - т.к. будут рассчитаны все партиции => все партиции попаду в кэш
   *
   * !!! Если после cache/persist будет show - рассчитана будет только одна партиция => одна партиция будет помещена в кэш
   * Т.е. часть данных будет в виде снэпшота в кэше, часть в источнике (csv-файл)
   * => при обновлении данных в источнике - теряется консистентность == Partial caching (антипаттерн)
   */
  onlyRuAndHigh.cache()
  val start2: Long = System.currentTimeMillis()
  onlyRuAndHigh.count()
  onlyRuAndHigh.show(numRows = 1, truncate = 100, vertical = true)
  onlyRuAndHigh.collect()

  onlyRuAndHigh
    .groupBy($"municipality")
    .count()
    .orderBy($"count".desc)
    .na.drop("any")

  val end2: Long = System.currentTimeMillis()
  println(s"time2: ${(end2.toDouble - start2) / 1000}")  // 0.6s
  onlyRuAndHigh.unpersist()
  println()

  /**
   * localcheckpoint ~= cache/persist
   *
   * Отличия localcheckpoint от cache/persist:
   * 1. Неленивое кэширование (по умолчанию)
   * 2. После localcheckpoint требуется очистка - Spark сам будет принимать решение о времени очистки кэша (не всегда эффективно)
   */

  /**
   * checkpoint
   *
   * сheckpoint - сериализует представление датафрейма и сохраняет на диск, с возможностью дальнейшего чтения
   * сheckpoint - стирает граф выполнения до него (как и localcheckpoint)
   * сheckpoint - проприетарная вещь, лучше сохранять данные в открытом формате (например - parquet)
   */

  /**
   * Память выделенная воркеру делится на:
   * 1. Расположение внутренних объектов Spark в хипе
   * 2. Storage (max 50%)
   *
   * Распределение памяти по областям является динамическим
   */


  /** Repartitioning */
  /** Количество партиций и распределение данных в них определяет разработчик коннектора */

  val skewColumn: Column = when(col("id") < 900, lit(0)).otherwise(lit(1))

  val skewDf: Dataset[lang.Long] =
    spark
      .range(0, 1000)  // col("id")
      // будет 2 непустых партиции c 900/100 элементами соответственно
      .repartition(10, skewColumn)

  skewDf.show()
  println(skewDf.count())
  println()

  def printItemPerPartition[T](ds: Dataset[T]): Unit = {
    val resDf: DataFrame =
      ds
        .mapPartitions { partition => Iterator(partition.length) }  // col("value")
        .withColumnRenamed("value", "itemPerPartition")

    resDf.show(50, truncate = false)
    println(resDf.count())
  }

  printItemPerPartition[java.lang.Long](skewDf)

  /**
   * Любые операции с датасетом skewDf будет работать медленно, т.к.
   * 1. Если суммарное количество потоков на всех воркерах больше 10, то в один момент времени работать будут максимум 10,
   * остальные будут простаивать.
   * 2. Из 10 партиций только в 2-х есть данные => только 2 потока будут обрабатывать данные,
   * при этом из-за перекоса данных между ними (900 vs 100) первый станет bottleneck'ом
   */

  /**
   * 1. RoundRobinRepartitioning - равномерное случайное перераспределение
   * Решает задачу восстановления распределения данных между партициями
   * Позволяет снизить количество партиций перед записью в базу/файл
   */
  val repartitionedDf1: Dataset[lang.Long] = skewDf.repartition(20)
  printItemPerPartition[java.lang.Long](repartitionedDf1)

  repartitionedDf1.explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Exchange RoundRobinPartitioning(20), REPARTITION_BY_NUM, [id=#565]
       +- Range (0, 1000, step=1, splits=8)
   */

  /**
   * 2. HashPartitioning - распределение по ключам заданных колонок
   * Позволяет оптимизировать граф вычислений
   * Пример - при репартицировании по полям, по которым в дальнейшем будет производиться группировка => повторного репатицирования не будет
   */

  /**
   * 3. RangePartitioning
   * orderBy/sortBy/show - выполняют репартицирование
   */

  /** sortWithinPartitions - не выполняет репатицирование, просто сортировка в рамках каждой партиции */
  val repartitionedDf2: Dataset[lang.Long] = skewDf.repartition(20, col("id"))
  printItemPerPartition[java.lang.Long](repartitionedDf2)

  repartitionedDf2.explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Exchange hashpartitioning(id#922L, 20), REPARTITION_BY_NUM, [id=#728]
       +- Range (0, 1000, step=1, splits=8)
   */

  /** coalesce */
  val coalescedDf: Dataset[lang.Long] = skewDf.coalesce(3)
  printItemPerPartition(coalescedDf)

  coalescedDf.explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Coalesce 3
       +- Exchange hashpartitioning(CASE WHEN (id#922L < 900) THEN 0 ELSE 1 END, 10), REPARTITION_BY_NUM, [id=#919]
          +- Range (0, 1000, step=1, splits=8)
   */


  /** DataSkew */
  airportsDf.printSchema()
  println(s"airportsDf partitions: ${airportsDf.rdd.getNumPartitions}")
  println()

  airportsDf
    .groupBy($"type")
    .count()
    .orderBy($"count".desc)
    .show()  // max records in partition ~34k

  /** Key salting */
  // saltModTen == 0 - 9
  val saltModTen: Column = pmod(round(rand() * 100, 0), lit(10)).cast("int")

  val saltedDf: DataFrame = airportsDf.withColumn("salt", saltModTen)
  saltedDf.show(numRows = 1, truncate = 200, vertical = true)
  println(s"saltedDf partitions: ${saltedDf.rdd.getNumPartitions}")
  println()

  /** 1-й этап - key salting */
  val firstStepDf: DataFrame =
    saltedDf
      .groupBy($"type", $"salt")
      .count()
      .orderBy($"count".desc)

  firstStepDf.show(200, truncate = false)  // max records in partition ~3.5k
  println(s"firstStepDf partitions: ${firstStepDf.rdd.getNumPartitions}")
  println()

  /** 2-й этап - финальная агрегация - суммирование */
  val secondStep: DataFrame =
    firstStepDf
      .groupBy($"type")
      .agg(sum("count").alias("count"))

  /**
   * Несмотря на то, что мы сделали 2 группировки вместо 1, распределение данных по воркерам стало более равномерным,
   * что позволило избежать OOM на воркерах
   */

  secondStep
    .orderBy($"count".desc)
    .show(200, truncate = false)

  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
