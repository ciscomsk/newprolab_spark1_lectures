package l_11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUI extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("l_11")
      .master("local[*]")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  val sparkUiUrl: Option[String] = sc.uiWebUrl
  println(sparkUiUrl)

  val appId: String = sc.applicationId
  println(appId)

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

  airportsDf.printSchema()
  airportsDf.show(numRows = 1, truncate = 100, vertical = true)

  airportsDf.count()

  airportsDf
    .localCheckpoint()
    .count()

  airportsDf
    .groupBy($"type")
    .count()
    .explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- HashAggregate(keys=[type#17], functions=[count(1)])
       +- Exchange hashpartitioning(type#17, 200), ENSURE_REQUIREMENTS, [id=#107]
          +- HashAggregate(keys=[type#17], functions=[partial_count(1)])
             +- FileScan csv [type#17] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<type:string>
   */

  airportsDf
    /**
     * Проекция колонки type будет вынесена на уровень чтения источника.
     * (1) Scan csv  - ReadSchema: struct<type:string>
     */
    .groupBy($"type")
    .count()
    .count()

  airportsDf.cache()  // кэширование - ленивая операция
//  airportsDf.show()  // !!! partial caching - закэшируется только 1 партиция
  airportsDf.count()

  airportsDf.unpersist()

  Thread.sleep(10000000)
}
