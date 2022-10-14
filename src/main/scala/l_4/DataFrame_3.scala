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

  val spark: SparkSession =
    SparkSession
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

  /**
   * !!! Несмотря на то, что onlyRuAndHigh является общим для всех действий ниже,
   * весь граф по его расчету будет выполняться при вызове каждого действия
   *
   * onlyRuAndHigh в нашем случае будет рассчитан 4 раза - count/show/collect/groupBy
   */
  val start1: Long = System.currentTimeMillis()
  onlyRuAndHigh.count()
  onlyRuAndHigh.show(numRows = 1, truncate = 100, vertical = true)
  onlyRuAndHigh.collect()

  onlyRuAndHigh
    .groupBy($"municipality")
    .count()
    .orderBy($"count".desc)
    .na.drop("any")
    .show()

  val end1: Long = System.currentTimeMillis()
  println(s"time1: ${(end1.toDouble - start1) / 1000}")  // 1.363s
  println()

  /**
   * !!! cache/persist - ленивые операции
   * => best practice - после cache/persist выполнять count - т.к. будут рассчитаны все партиции => все партиции попаду в кэш
   *
   * !!! Если после cache/persist будет show => рассчитана будет только 1 партиция => 1 партиция будет помещена в кэш
   * Т.е. часть данных будет в виде снэпшота в кэше, часть в источнике (csv-файл)
   * => при обновлении данных в источнике - теряется консистентность == partial caching (антипаттерн)
   *
   * После окончания работы с закешированными данными необходимо выполнить unpersist - для очистки памяти
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
    .show()

  val end2: Long = System.currentTimeMillis()
  println(s"time2: ${(end2.toDouble - start2) / 1000}")  // 0.63s
  onlyRuAndHigh.unpersist()
  println()

  /**
   * localcheckpoint ~= cache/persist
   *
   * Отличия localcheckpoint от cache/persist:
   * 1. localcheckpoint - неленивая операция (по умолчанию)
   * 2. После localcheckpoint требуется очистка - Spark сам будет принимать решение о времени очистки кэша (не всегда эффективно)
   * 3. localcheckpoint  - стирает граф выполнения до localcheckpoint
   */

  /**
   * checkpoint
   *
   * сheckpoint - сериализует представление датафрейма и сохраняет на диск, с возможностью дальнейшего чтения (в т.ч. другим spark приложением)
   * сheckpoint - стирает граф выполнения до сheckpoint (как и localcheckpoint)
   * сheckpoint - проприетарная вещь, лучше сохранять данные в открытом формате (например - parquet)
   */

  /**
   * Память выделенная воркеру делится на:
   * 1. Расположение внутренних объектов Spark в хипе
   * 2. Storage (max 50%)
   *
   * Распределение памяти по областям является динамическим
   */


  /** Repartition */
  val skewColumn: Column = when(col("id") < 900, lit(0)).otherwise(lit(1))

  val skewDs: Dataset[lang.Long] =
    spark
      .range(0, 1000)  // col("id")
      // будет 2 непустых партиции c 900/100 элементами соответственно
      .repartition(10, skewColumn)

  skewDs.show()
  println(skewDs.count())
  println()

  def printItemPerPartition[T](ds: Dataset[T]): Unit = {
    val resDf: DataFrame =
      ds
        .mapPartitions { partition => Iterator(partition.length) }  // col("value")
        .withColumnRenamed("value", "itemPerPartition")

    resDf.show(50, truncate = false)
    println(resDf.count())
  }

  printItemPerPartition[java.lang.Long](skewDs)
  println()

  /**
   * Любые операции с датасетом skewDs будет работать медленно, т.к.:
   * 1. Если суммарное количество потоков на всех воркерах больше 10, то в один момент времени работать будут максимум 10,
   * остальные будут простаивать
   * 2. Из 10 партиций только в 2-х есть данные => только 2 потока будут обрабатывать данные,
   * при этом из-за перекоса данных между ними (900 vs 100) первый станет bottleneck'ом
   */

  /**
   * 1. RoundRobinRepartitioning - равномерное случайное перераспределение
   * Решает задачу восстановления распределения данных между партициями
   * Позволяет снизить количество партиций перед записью в базу/файл
   */
  val repartitionedDf1: Dataset[lang.Long] = skewDs.repartition(20)
  printItemPerPartition[java.lang.Long](repartitionedDf1)
  println()

  repartitionedDf1.explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Exchange RoundRobinPartitioning(20), REPARTITION_BY_NUM, [id=#698]
       +- Range (0, 1000, step=1, splits=8)
   */

  /**
   * 2. HashPartitioning - распределение по ключам заданных колонок
   * Позволяет оптимизировать граф вычислений
   * Пример - при репартицировании по полям, по которым в дальнейшем будет производиться группировка => повторного репартицирования не будет
   */

  val repartitionedDf2: Dataset[lang.Long] = skewDs.repartition(20, col("id"))
  printItemPerPartition[java.lang.Long](repartitionedDf2)
  println()

  repartitionedDf2.explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Exchange hashpartitioning(id#1247L, 20), REPARTITION_BY_NUM, [id=#861]
       +- Range (0, 1000, step=1, splits=8)
   */

  /**
   * 3. RangePartitioning
   * orderBy/sortBy - выполняют репартицирование
   *
   * sortWithinPartitions - не выполняет репатицирование, сортировка выполняется в рамках каждой партиции
   */


  /** coalesce */
  val coalescedDf: Dataset[lang.Long] = skewDs.coalesce(3)
  printItemPerPartition(coalescedDf)
  println()

  coalescedDf.explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Coalesce 3
       +- Exchange hashpartitioning(CASE WHEN (id#1247L < 900) THEN 0 ELSE 1 END, 10), REPARTITION_BY_NUM, [id=#1045]
          +- Range (0, 1000, step=1, splits=8)
   */


  /** Data skew */
  airportsDf.printSchema()
  println(s"airportsDf partitions: ${airportsDf.rdd.getNumPartitions}")
  println()

  airportsDf
    .groupBy($"type")
    .count()
    .orderBy($"count".desc)
    /** max records in partition ~34k */
    .show()

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

  /** max records in partition ~3.5k */
  firstStepDf.show(200, truncate = false)
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
