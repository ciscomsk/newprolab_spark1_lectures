package l_11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUI extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("l_11")
      .master("local[*]")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  import spark.implicits._

  val sparkUiUrl: Option[String] = sc.uiWebUrl
  val appId: String = sc.applicationId
  println(sparkUiUrl)
  println(appId)
  println()

  /*
    curl -s http://192.168.1.252:4043/api/v1/applications
    curl -s http://192.168.1.252:4043/api/v1/applications/local-1716801784573
   */

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
    +- HashAggregate(keys=[type#18], functions=[count(1)])
       +- Exchange hashpartitioning(type#18, 200), ENSURE_REQUIREMENTS, [plan_id=107]
          +- HashAggregate(keys=[type#18], functions=[partial_count(1)])
             +- FileScan csv [type#18] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<type:string>
   */

  airportsDf
    /**
     * проекция колонки type будет вынесена на уровень чтения источника
     * (1) Scan csv - ReadSchema: struct<type:string>
     */
    .groupBy($"type")
    .count()
    .count()

  /** !!! кэширование - ленивая операция, выполняется после первого action */
  airportsDf.cache()

  /** !!! Partial caching - закэшируется только 1 партиция */
//  airportsDf.show()

  airportsDf.count()

//  airportsDf.unpersist()


  println(sc.uiWebUrl)
  Thread.sleep(1_000_000)

  spark.stop()
}
