package l_4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, count, struct, sum, to_json}

object DataFrame_2 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("DataFrame_2")
    .getOrCreate

  import spark.implicits._

  val cleanData: DataFrame = spark
    .read
    .load("src/main/resources/l_4/cleandata")

  cleanData.show

  /** groupBy вызывает репартицирование (сохранение на диске). */
  val aggCount: DataFrame = cleanData
    .groupBy('continent)  // после groupBy получаем RelationalGroupedDataset
    .count

  aggCount.show

  val aggSum: DataFrame = cleanData
    .groupBy('continent)
    .sum("population")

  aggSum.show

  /** agg - позволяет рассчитать несколько агрегатов. */
  val agg: DataFrame = cleanData
    .groupBy('continent)
    .agg(
      count("*").alias("count"),
      sum("population").alias("sumPop")
    )

  agg.show

  /** collect_list - собирает все значения в массив. */
  val aggList: DataFrame = cleanData
    .groupBy('continent)
    .agg(collect_list("country").alias("countries"))

  aggList.show()
  aggList.printSchema()
  aggList.show(numRows = 10, truncate = 100, vertical = true)

  val withStruct: DataFrame = aggList.select(struct('continent, 'countries).alias("s"))
  withStruct.show(10, truncate = false)
  withStruct.printSchema()

  val json1: DataFrame = withStruct.withColumn("s", to_json('s))
  json1.show(10, truncate = false)
  json1.printSchema

  json1.explain
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [to_json(struct(continent, continent#0, countries, countries#101), Some(Europe/Moscow)) AS s#156]
       +- ObjectHashAggregate(keys=[continent#0], functions=[collect_list(country#1, 0, 0)])
          +- Exchange hashpartitioning(continent#0, 200), ENSURE_REQUIREMENTS, [id=#417]
             +- ObjectHashAggregate(keys=[continent#0], functions=[partial_collect_list(country#1, 0, 0)])
                +- FileScan parquet [continent#0,country#1] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<continent:string,country:string>
   */

  /** toJSON - преобразует все колонки DF в JSON. */
  val json2: Dataset[String] = aggList.toJSON
  json2.show(5, truncate = false)

  json2.explain
  /**
   * Проблема toJSON (и других методов) из Dataset API - под капотом выполняется:
   * DeserializeToObject (Internal row => Java object) => MapPartitions => SerializeFromObject (Java object => Internal row)
   */
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false) AS value#174]
       +- MapPartitions org.apache.spark.sql.Dataset$$Lambda$3273/0x000000080144f040@360ccb68, obj#173: java.lang.String
          +- DeserializeToObject createexternalrow(continent#0.toString, staticinvoke(class scala.collection.mutable.WrappedArray$, ObjectType(interface scala.collection.Seq), make, mapobjects(lambdavariable(MapObject, StringType, true, -1), lambdavariable(MapObject, StringType, true, -1).toString, countries#101, None).array, true, false), StructField(continent,StringType,true), StructField(countries,ArrayType(StringType,false),false)), obj#172: org.apache.spark.sql.Row
             +- ObjectHashAggregate(keys=[continent#0], functions=[collect_list(country#1, 0, 0)])
                +- Exchange hashpartitioning(continent#0, 200), ENSURE_REQUIREMENTS, [id=#523]
                   +- ObjectHashAggregate(keys=[continent#0], functions=[partial_collect_list(country#1, 0, 0)])
                      +- FileScan parquet [continent#0,country#1] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<continent:string,country:string>
   */

  val pivot: DataFrame = cleanData
    .groupBy(col("country"))
    .pivot("continent")
    .agg(sum("population"))

  pivot.show

//  spark.conf.set("spark.sql.shuffle.partitions", 150)

  cleanData
    .localCheckpoint
    .groupBy(col("country"))
    .pivot("continent")
    .agg(sum("population"))
    .explain
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    // Агрегация внутри каждой партиции.
    +- HashAggregate(keys=[country#1], functions=[pivotfirst(continent#0, sum(population)#315L, Africa, Europe, Undefined, 0, 0)])
       // Репартиционирование по country
       +- Exchange hashpartitioning(country#1, 200), ENSURE_REQUIREMENTS, [id=#875]
          // Агрегация внутри каждой партиции.
          +- HashAggregate(keys=[country#1], functions=[partial_pivotfirst(continent#0, sum(population)#315L, Africa, Europe, Undefined, 0, 0)])
             // Агрегация внутри каждой партиции.
             +- HashAggregate(keys=[country#1, continent#0], functions=[sum(population#3L)])
                // Репартиционирование по country + continent
                +- Exchange hashpartitioning(country#1, continent#0, 200), ENSURE_REQUIREMENTS, [id=#871]
                   // Агрегация внутри каждой партиции.
                   +- HashAggregate(keys=[country#1, continent#0], functions=[partial_sum(population#3L)])
                      // Выбираем только колонки, использующиеся в агрегации.
                      +- Project [continent#0, country#1, population#3L]
                         +- Scan ExistingRDD[continent#0,country#1,name#2,population#3L]
   */

  spark.stop
}
