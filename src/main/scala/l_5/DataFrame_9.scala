package l_5

import l_5.DataFrame_5.printPhysicalPlan
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.lang

object DataFrame_9 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  import spark.implicits._

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

//  airportsDf
//    .write
//    .format("parquet")
//    .partitionBy("iso_country")
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/l_5/airports")
//
//  airportsDf
//    .write
//    .format("json")
//    .partitionBy("iso_country")
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/l_5/airports_json")

  val airportPartPqDf: DataFrame =
    spark
      .read
      .parquet("src/main/resources/l_5/airports")

  println()
  airportPartPqDf.printSchema()
  println()

  println("__Column projection__: ")
  /**
   * 1. Column projection
   * в плане - ReadSchema
   */
  spark.time {
    val selectedDf: DataFrame = airportPartPqDf.select($"ident")

    selectedDf.cache()
    /** в кэш будет помещена только колонка ident */
    selectedDf.count()
    selectedDf.unpersist()

    printPhysicalPlan(selectedDf)
    /*
      *(1) Project [ident#41]
      +- *(1) ColumnarToRow
         // ReadSchema: struct<ident:string> - будет вычитана только колонка ident
         +- FileScan parquet [ident#41,iso_country#52] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string>
     */
  } // 1264 ms
  println()

  spark.time {
    val selectedDf: DataFrame = airportPartPqDf

    selectedDf.cache()
    selectedDf.count()
    selectedDf.unpersist()

    printPhysicalPlan(selectedDf)
    /*
      *(1) ColumnarToRow
      // ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...
      +- FileScan parquet [ident#41,type#42,name#43,elevation_ft#44,continent#45,iso_region#46,municipality#47,gps_code#48,iata_code#49,local_code#50,coordinates#51,iso_country#52] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...
     */
  } // 854 ms
  println()

  /** !!! для неколоночных форматов (например json) - ReadSchema будет указана в плане выполнения, но работать оптимизация не будет */
  spark
    .read
    .json("src/main/resources/l_5/airports_json")
    .select($"ident")
    .explain()
  /*
    == Physical Plan ==
    *(1) Project [ident#493]
    // ReadSchema: struct<ident:string>
    +- FileScan json [ident#493,iso_country#499] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string>
   */


  println("__Partition pruning__: ")
  /**
   * 2. Partition pruning - df.write.partitionBy("iso_country")
   * в плане - PartitionFilters
   */
  spark.time {
    val filteredDf: Dataset[Row] = airportPartPqDf.filter($"iso_country" === "RU")
//    val filteredDf: Dataset[Row] = airportPartPqDf.filter($"iso_country" > "RU")
//    val filteredDf: Dataset[Row] = airportPartPqDf.filter($"iso_country" === "RU" || $"iso_country" === "US")
//    println(filteredDf.queryExecution.executedPlan.toJSON)

    filteredDf.count()

    printPhysicalPlan(filteredDf)
    /*
      *(1) ColumnarToRow
      // PartitionFilters: [isnotnull(iso_country#52), (iso_country#52 = RU)] -> будет прочитан только каталог RU
      +- FileScan parquet [ident#41,type#42,name#43,elevation_ft#44,continent#45,iso_region#46,municipality#47,gps_code#48,iata_code#49,local_code#50,coordinates#51,iso_country#52] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [isnotnull(iso_country#52), (iso_country#52 = RU)], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...
     */
  } // 117 ms
  println()


  spark.time {
    val filteredDf: Dataset[Row] = airportPartPqDf
    filteredDf.count()

    printPhysicalPlan(filteredDf)
    /*
      *(1) ColumnarToRow
      // PartitionFilters: []
      +- FileScan parquet [ident#41,type#42,name#43,elevation_ft#44,continent#45,iso_region#46,municipality#47,gps_code#48,iata_code#49,local_code#50,coordinates#51,iso_country#52] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...

     */
  } // 256 ms
  println()


  println("__Predicate pushdown__: ")
  /**
   * 3. Predicate pushdown
   * в плане - PushedFilters
   */
  spark.time {
    val filteredDf: Dataset[Row] = airportPartPqDf.filter($"iso_region" === "RU")
    filteredDf.count()

    printPhysicalPlan(filteredDf)
    /*
      // Filter (isnotnull(iso_region#46) AND (iso_region#46 = RU))
      *(1) Filter (isnotnull(iso_region#46) AND (iso_region#46 = RU))
      +- *(1) ColumnarToRow
         // PushedFilters: [IsNotNull(iso_region), EqualTo(iso_region,RU)]
         +- FileScan parquet [ident#41,type#42,name#43,elevation_ft#44,continent#45,iso_region#46,municipality#47,gps_code#48,iata_code#49,local_code#50,coordinates#51,iso_country#52] Batched: true, DataFilters: [isnotnull(iso_region#46), (iso_region#46 = RU)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [IsNotNull(iso_region), EqualTo(iso_region,RU)], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...
     */
  } // 386 ms
  println()


  println("__Simplify casts__: ")
  /**
   * 4. Simplify casts
   * LongType.cast(LongType) -> каста не будет
   */
  val resDf1: DataFrame =
    spark
      .range(0, 10)
      .select($"id".cast(LongType))

  printPhysicalPlan(resDf1)
  /*
    *(1) Range (0, 10, step=1, splits=8)
   */

  resDf1.explain(true)
  /*
    == Parsed Logical Plan ==
    'Project [unresolvedalias(cast('id as bigint), None)]
    +- Range (0, 10, step=1, splits=Some(8))

    == Analyzed Logical Plan ==
    id: bigint
    Project [cast(id#565L as bigint) AS id#567L]
    +- Range (0, 10, step=1, splits=Some(8))

    == Optimized Logical Plan ==
    Range (0, 10, step=1, splits=Some(8))

    == Physical Plan ==
    *(1) Range (0, 10, step=1, splits=8)
   */

  /** cast LongType -> IntegerType -> LongType - оптимизация работать не будет */
  val resDf2: DataFrame =
    spark
      .range(0, 10)
      .select($"id".cast(IntegerType).cast(LongType))

  printPhysicalPlan(resDf2)
  /*
    *(1) Project [cast(cast(id#569L as int) as bigint) AS id#572L]
    +- *(1) Range (0, 10, step=1, splits=8)
   */


  println("__Constant folding__: ")
  /**
   * 5. Constant folding
   * lit(3) > lit(0) -> true
   */
  val resDf3: Dataset[Row] =
    spark
      .range(0, 10)
      .select((lit(3) > lit(0)).alias("foo"))

  printPhysicalPlan(resDf3)
  /*
    *(1) Project [true AS foo#576]
    +- *(1) Range (0, 10, step=1, splits=8)
   */

  resDf3.explain(true)
  /*
    == Parsed Logical Plan ==
    Project [(3 > 0) AS foo#576]
    +- Range (0, 10, step=1, splits=Some(8))

    == Analyzed Logical Plan ==
    foo: boolean
    Project [(3 > 0) AS foo#576]
    +- Range (0, 10, step=1, splits=Some(8))

    == Optimized Logical Plan ==
    Project [true AS foo#576]
    +- Range (0, 10, step=1, splits=Some(8))

    == Physical Plan ==
    *(1) Project [true AS foo#576]
    +- *(1) Range (0, 10, step=1, splits=8)
   */

  val resDf4: DataFrame =
    spark
      .range(0, 10)
      .select(($"id" > 0).alias("foo"))

  printPhysicalPlan(resDf4)
  /*
    *(1) Project [(id#578L > 0) AS foo#580]
    +- *(1) Range (0, 10, step=1, splits=8)
   */


  println("__Combine filters__: ")
  /**
   * 6. Combine filters
   * filter($"id" > 0) + filter($"id" =!= 5) + filter($"id" < 10) -> Filter ((id#582L > 0) AND (NOT (id#582L = 5) AND (id#582L < 10)))
   */
  val resDf5: Dataset[Row] =
    spark
      .range(0, 10)
      .filter($"id" > 0)
      /** проекция не мешает объединению фильтров */
      .select(col("*"))
      .filter($"id" =!= 5)
      .filter($"id" < 10)

  printPhysicalPlan(resDf5)
  /*
    *(1) Filter ((id#582L > 0) AND (NOT (id#582L = 5) AND (id#582L < 10)))
    +- *(1) Range (0, 10, step=1, splits=8)
   */

  resDf5.explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter ('id < 10)
    +- Filter NOT (id#582L = cast(5 as bigint))
       +- Project [id#582L]
          +- Filter (id#582L > cast(0 as bigint))
             +- Range (0, 10, step=1, splits=Some(8))

    == Analyzed Logical Plan ==
    id: bigint
    Filter (id#582L < cast(10 as bigint))
    +- Filter NOT (id#582L = cast(5 as bigint))
       +- Project [id#582L]
          +- Filter (id#582L > cast(0 as bigint))
             +- Range (0, 10, step=1, splits=Some(8))

    == Optimized Logical Plan ==
    Filter ((id#582L > 0) AND (NOT (id#582L = 5) AND (id#582L < 10)))
    +- Range (0, 10, step=1, splits=Some(8))

    == Physical Plan ==
    *(1) Filter ((id#582L > 0) AND (NOT (id#582L = 5) AND (id#582L < 10)))
    +- *(1) Range (0, 10, step=1, splits=8)
   */


  println(sc.uiWebUrl)
  Thread.sleep(1_000_000)

  spark.stop()
}
