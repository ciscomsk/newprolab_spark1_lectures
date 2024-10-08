package l_4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, count, struct, sum, to_json}

object DataFrame_2 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("DataFrame_2")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  import spark.implicits._

  val cleanDataDf: DataFrame =
    spark
      .read
      .load("src/main/resources/l_4/cleandata")

  cleanDataDf.show()

  /**
   * groupBy - запускает шафл
   * результат groupBy - RelationalGroupedDataset
   */
  val aggCountDf: DataFrame =
    cleanDataDf
      .groupBy($"continent")
      .count()

  aggCountDf.show()

  val aggSumDf: DataFrame =
    cleanDataDf
      .groupBy($"continent")
      .sum("population")

  aggSumDf.show()

  /** agg - позволяет рассчитать несколько агрегатов */
  val aggDf: DataFrame =
    cleanDataDf
      .groupBy($"continent")
      .agg(
        count("*").as("count"),
        sum("population").as("sum_pop")
      )

  aggDf.show()

  /** collect_list - собрать все значения в массив */
  val aggListDf: DataFrame =
    cleanDataDf
      .groupBy($"continent")
      .agg(collect_list("country").as("countries"))

  aggListDf.show(truncate = false)
  aggListDf.printSchema()
  /*
    root
     |-- continent: string (nullable = true)
     |-- countries: array (nullable = false)
     |    |-- element: string (containsNull = false)
   */
  aggListDf.show(numRows = 10, truncate = 100, vertical = true)

  // col("s.*") - выборка всех колонки структуры s
  val structDf: DataFrame = aggListDf.select(struct($"continent", $"countries").as("s"))
  println("structDf: ")
  structDf.show(10, truncate = false)
  structDf.printSchema()
  /*
    root
     |-- s: struct (nullable = false)
     |    |-- continent: string (nullable = true)
     |    |-- countries: array (nullable = false)
     |    |    |-- element: string (containsNull = false)
   */

  /** to_json - удобен при передаче данных в Kafka */
  val json1Df: DataFrame = structDf.withColumn("s", to_json($"s"))
  println("json1Df: ")
  json1Df.show(10, truncate = false)
  json1Df.printSchema()
  /*
    root
     |-- s: string (nullable = true)
   */

  json1Df.explain()
  /*
    // в плане нет struct + to_json (?)
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- ObjectHashAggregate(keys=[continent#0], functions=[collect_list(country#1, 0, 0)])
       +- Exchange hashpartitioning(continent#0, 200), ENSURE_REQUIREMENTS, [plan_id=337]
          +- ObjectHashAggregate(keys=[continent#0], functions=[partial_collect_list(country#1, 0, 0)])
             +- FileScan parquet [continent#0,country#1] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<continent:string,country:string>
   */

  /** toJSON - преобразует все колонки датафрейма в json */
  val json2Ds: Dataset[String] = aggListDf.toJSON
  println("json2Ds: ")
  json2Ds.show(5, truncate = false)

  json2Ds.explain()
  /**
   * !!! проблема toJSON (и других методов Dataset API) - выполнение дополнительных сериализаций/десериализаций:
   * DeserializeToObject (Internal row -> Java object) -> MapPartitions -> SerializeFromObject (Java object -> Internal row)
   */
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    // конвертация Java object (String) -> Internal row
    +- SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false, true) AS value#175]
       // применение функции к Java object (String)
       +- MapPartitions org.apache.spark.sql.Dataset$$Lambda$3555/0x00000008015cf040@2a0cc666, obj#174: java.lang.String
          // конвертация Internal row -> Java object (String)
          +- DeserializeToObject createexternalrow(continent#0.toString, mapobjects(lambdavariable(MapObject, StringType, false, -1), lambdavariable(MapObject, StringType, false, -1).toString, countries#102, Some(class scala.collection.mutable.ArraySeq)), StructField(continent,StringType,true), StructField(countries,ArrayType(StringType,false),false)), obj#173: org.apache.spark.sql.Row
             +- ObjectHashAggregate(keys=[continent#0], functions=[collect_list(country#1, 0, 0)])
                +- Exchange hashpartitioning(continent#0, 200), ENSURE_REQUIREMENTS, [plan_id=453]
                   +- ObjectHashAggregate(keys=[continent#0], functions=[partial_collect_list(country#1, 0, 0)])
                      +- FileScan parquet [continent#0,country#1] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<continent:string,country:string>
   */

  /** pivot - создает колонки из значений заданной колонки  */
  val pivotDf: DataFrame =
  cleanDataDf
    .groupBy(col("country"))
    .pivot("continent")
    .agg(sum("population"))

  println("pivotDf: ")
  pivotDf.show()
  /*
    cleanDataDf:
    +---------+-------+---------+----------+
    |continent|country|     name|population|
    +---------+-------+---------+----------+
    |   Africa|  Egypt|    Cairo|  11922948|
    |   Europe| France|    Paris|   2196936|
    |   Europe|Germany|   Berlin|   3490105|
    |   Europe| Russia|   Moscow|  12380664|
    |   Europe|  Spain|Barselona|         0|
    |Undefined|  Spain|   Madrid|         0|
    +---------+-------+---------+----------+

    ->

    pivotDf:
    +-------+--------+--------+---------+
    |country|  Africa|  Europe|Undefined|
    +-------+--------+--------+---------+
    | Russia|    NULL|12380664|     NULL|
    |Germany|    NULL| 3490105|     NULL|
    | France|    NULL| 2196936|     NULL|
    |  Spain|    NULL|       0|        0|
    |  Egypt|11922948|    NULL|     NULL|
    +-------+--------+--------+---------+
   */

  /** spark.sql.shuffle.partitions = 200 (по умолчанию) */
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
       // агрегация внутри каждой партиции после репартицирование - по ключу country рассчитывается pivotfirst
       +- HashAggregate(keys=[country#1], functions=[pivotfirst(continent#0, sum(population)#316L, Africa, Europe, Undefined, 0, 0)])
          // репартицирование по country
          +- Exchange hashpartitioning(country#1, 200), ENSURE_REQUIREMENTS, [plan_id=796]
             // агрегация внутри каждой партиции - по ключу country рассчитывается partial_pivotfirst
             +- HashAggregate(keys=[country#1], functions=[partial_pivotfirst(continent#0, sum(population)#316L, Africa, Europe, Undefined, 0, 0)])
                // агрегация внутри каждой партиции после репартицирования - по ключам country + continent рассчитывается sum(population)
                +- HashAggregate(keys=[country#1, continent#0], functions=[sum(population#3L)])
                   // репартицирование по country + continent
                   +- Exchange hashpartitioning(country#1, continent#0, 200), ENSURE_REQUIREMENTS, [plan_id=792]
                      // агрегация внутри каждой партиции - по ключам country + continent рассчитывается partial_sum(population)
                      +- HashAggregate(keys=[country#1, continent#0], functions=[partial_sum(population#3L)])
                         // выбираем только колонки, использующиеся в агрегации
                         +- Project [continent#0, country#1, population#3L]
                            +- Scan ExistingRDD[continent#0,country#1,name#2,population#3L]
   */


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
