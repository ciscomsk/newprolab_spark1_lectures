package l_4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, count, struct, sum, to_json}

object DataFrame_2 extends App {
  // не работает в Spark 3.3.0
//  Logger
//    .getLogger("org")
//    .setLevel(Level.OFF)

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("DataFrame_2")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  val cleanDataDf: DataFrame =
    spark
      .read
      .load("src/main/resources/l_4/cleandata")

  cleanDataDf.show()

  /** groupBy вызывает репартицирование (сохранение на диске) */
  val aggCount: DataFrame =
    cleanDataDf
      .groupBy($"continent")  // после groupBy получаем RelationalGroupedDataset
      .count()

  aggCount.show()

  val aggSum: DataFrame =
    cleanDataDf
      .groupBy($"continent")
      .sum("population")

  aggSum.show()

  /** agg - позволяет рассчитать несколько агрегатов */
  val agg: DataFrame =
    cleanDataDf
    .groupBy($"continent")
    .agg(
      count("*").alias("count"),
      sum("population").alias("sumPop")
    )

  agg.show()

  /** collect_list - собирает все значения в массив. */
  val aggList: DataFrame =
    cleanDataDf
      .groupBy($"continent")
      .agg(collect_list("country").alias("countries"))

  aggList.show()
  aggList.printSchema()
  aggList.show(numRows = 10, truncate = 100, vertical = true)

  val withStruct: DataFrame = aggList.select(struct($"continent", $"countries").alias("s"))
  withStruct.show(10, truncate = false)
  withStruct.printSchema()

  /** to_json - удобен при передаче в Kafka */
  val json1: DataFrame = withStruct.withColumn("s", to_json($"s"))
  json1.show(10, truncate = false)
  json1.printSchema()

  json1.explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- ObjectHashAggregate(keys=[continent#0], functions=[collect_list(country#1, 0, 0)])
       +- Exchange hashpartitioning(continent#0, 200), ENSURE_REQUIREMENTS, [id=#337]
          +- ObjectHashAggregate(keys=[continent#0], functions=[partial_collect_list(country#1, 0, 0)])
             +- FileScan parquet [continent#0,country#1] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn_2/courses/spark/newprolab/spark_1/_repos/lectur..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<continent:string,country:string>
   */

  /** toJSON - преобразует все колонки DF в json */
  val json2: Dataset[String] = aggList.toJSON
  json2.show(5, truncate = false)

  json2.explain()
  /**
   * Проблема toJSON (и других методов) из Dataset API - под капотом выполняется:
   * DeserializeToObject (Internal row => Java object) => MapPartitions => SerializeFromObject (Java object => Internal row)
   */
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false, true) AS value#175]
       +- MapPartitions org.apache.spark.sql.Dataset$$Lambda$3362/0x0000000801491040@4ed096cc, obj#174: java.lang.String
          +- DeserializeToObject createexternalrow(continent#0.toString, staticinvoke(class scala.collection.mutable.ArraySeq$, ObjectType(interface scala.collection.Seq), make, mapobjects(lambdavariable(MapObject, StringType, true, -1), lambdavariable(MapObject, StringType, true, -1).toString, countries#102, None).array, true, false, true), StructField(continent,StringType,true), StructField(countries,ArrayType(StringType,false),false)), obj#173: org.apache.spark.sql.Row
             +- ObjectHashAggregate(keys=[continent#0], functions=[collect_list(country#1, 0, 0)])
                +- Exchange hashpartitioning(continent#0, 200), ENSURE_REQUIREMENTS, [id=#442]
                   +- ObjectHashAggregate(keys=[continent#0], functions=[partial_collect_list(country#1, 0, 0)])
                      +- FileScan parquet [continent#0,country#1] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn_2/courses/spark/newprolab/spark_1/_repos/lectur..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<continent:string,country:string>
   */

  val pivot: DataFrame =
  cleanDataDf
    .groupBy(col("country"))
    .pivot("continent")
    .agg(sum("population"))

  pivot.show()

//  spark.conf.set("spark.sql.shuffle.partitions", 150)

  cleanDataDf
    .localCheckpoint()
    .groupBy(col("country"))
    .pivot("continent")
    .agg(sum("population"))
    .explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [country#1, __pivot_sum(population) AS `sum(population)`#324[0] AS Africa#325L, __pivot_sum(population) AS `sum(population)`#324[1] AS Europe#326L, __pivot_sum(population) AS `sum(population)`#324[2] AS Undefined#327L]
       // Агрегация внутри каждой партиции после репартиционирования
       +- HashAggregate(keys=[country#1], functions=[pivotfirst(continent#0, sum(population)#316L, Africa, Europe, Undefined, 0, 0)])
          // Репартиционирование по country
          +- Exchange hashpartitioning(country#1, 200), ENSURE_REQUIREMENTS, [id=#785]
             // Агрегация внутри каждой партиции
             +- HashAggregate(keys=[country#1], functions=[partial_pivotfirst(continent#0, sum(population)#316L, Africa, Europe, Undefined, 0, 0)])
                // Агрегация внутри каждой партиции после репартиционирования
                +- HashAggregate(keys=[country#1, continent#0], functions=[sum(population#3L)])
                   // Репартиционирование по country + continent
                   +- Exchange hashpartitioning(country#1, continent#0, 200), ENSURE_REQUIREMENTS, [id=#781]
                      // Агрегация внутри каждой партиции
                      +- HashAggregate(keys=[country#1, continent#0], functions=[partial_sum(population#3L)])
                         // Выбираем только колонки, использующиеся в агрегации
                         +- Project [continent#0, country#1, population#3L]
                            +- Scan ExistingRDD[continent#0,country#1,name#2,population#3L]
   */

  spark.stop()
}
