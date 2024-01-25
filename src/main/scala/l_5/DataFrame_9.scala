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

  println("__Column projection: ")
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
      *(1) Project [ident#92]
      +- *(1) ColumnarToRow
         // ReadSchema: struct<ident:string> - будет вычитана только колонка ident
         +- FileScan parquet [ident#92,iso_country#103] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string>
     */
  } // 1002 ms
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
      +- FileScan parquet [ident#92,type#93,name#94,elevation_ft#95,continent#96,iso_region#97,municipality#98,gps_code#99,iata_code#100,local_code#101,coordinates#102,iso_country#103] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...
     */
  } // 914 ms
  println()

  /** !!! Для текстовых форматов (например json) - ReadSchema будет указана в плане выполнения, но работать оптимизация не будет */
  spark
    .read
    .json("src/main/resources/l_5/airports_json")
    .select($"ident")
    .explain()
  /*
    == Physical Plan ==
    *(1) Project [ident#544]
    // ReadSchema: struct<ident:string>
    +- FileScan json [ident#544,iso_country#550] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string>
   */


  println("__Partition pruning: ")
  /**
   * 2. Partition pruning - df.write.partitionBy("iso_country")
   * в плане - PartitionFilters
   */
  spark.time {
    val filteredDf: Dataset[Row] = airportPartPqDf.filter($"iso_country" > "RU")
//    val filteredDf: Dataset[Row] = airportPartPqDf.filter($"iso_country" === "RU" || $"iso_country" === "US")
//    println(filteredDf.queryExecution.executedPlan.toJSON)

    filteredDf.count()

    printPhysicalPlan(filteredDf)
    /*
      *(1) ColumnarToRow
      // PartitionFilters: [isnotnull(iso_country#103), (iso_country#103 = RU)] => будет прочитан только каталог RU
      +- FileScan parquet [ident#92,type#93,name#94,elevation_ft#95,continent#96,iso_region#97,municipality#98,gps_code#99,iata_code#100,local_code#101,coordinates#102,iso_country#103] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [isnotnull(iso_country#103), (iso_country#103 = RU)], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...
     */
  } // 156 ms
  println()



  spark.time {
    val filteredDf: Dataset[Row] = airportPartPqDf
    filteredDf.count()

    printPhysicalPlan(filteredDf)
    /*
      *(1) ColumnarToRow
      // PartitionFilters: []
      +- FileScan parquet [ident#92,type#93,name#94,elevation_ft#95,continent#96,iso_region#97,municipality#98,gps_code#99,iata_code#100,local_code#101,coordinates#102,iso_country#103] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...
     */
  } // 253 ms
  println()


  println("__Predicate pushdown: ")
  /**
   * 3. Predicate pushdown
   * в плане - PushedFilters
   */
  spark.time {
    val filteredDf: Dataset[Row] = airportPartPqDf.filter($"iso_region" === "RU")
    filteredDf.count()

    printPhysicalPlan(filteredDf)
    /*
      // Filter (isnotnull(iso_region#97) AND (iso_region#97 = RU))
      *(1) Filter (isnotnull(iso_region#97) AND (iso_region#97 = RU))
      +- *(1) ColumnarToRow
         // PushedFilters: [IsNotNull(iso_region), EqualTo(iso_region,RU)]
         +- FileScan parquet [ident#92,type#93,name#94,elevation_ft#95,continent#96,iso_region#97,municipality#98,gps_code#99,iata_code#100,local_code#101,coordinates#102,iso_country#103] Batched: true, DataFilters: [isnotnull(iso_region#97), (iso_region#97 = RU)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [IsNotNull(iso_region), EqualTo(iso_region,RU)], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...
     */
  } // 364 ms
  println()


  println("__Simplify casts: ")
  /**
   * 4. Simplify casts
   * LongType.cast(LongType) => каста не будет
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
    Project [cast(id#616L as bigint) AS id#618L]
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
    *(1) Project [cast(cast(id#620L as int) as bigint) AS id#623L]
    +- *(1) Range (0, 10, step=1, splits=8)
   */


  println("__Constant folding: ")
  /**
   * 5. Constant folding
   * (lit(3) > lit(0) => true
   */
  val resDf3: Dataset[Row] =
    spark
      .range(0, 10)
      .select((lit(3) > lit(0)).alias("foo"))

  printPhysicalPlan(resDf3)
  /*
    *(1) Project [true AS foo#627]
    +- *(1) Range (0, 10, step=1, splits=8)
   */

  resDf3.explain(true)
  /*
    == Parsed Logical Plan ==
    Project [(3 > 0) AS foo#627]
    +- Range (0, 10, step=1, splits=Some(8))

    == Analyzed Logical Plan ==
    foo: boolean
    Project [(3 > 0) AS foo#627]
    +- Range (0, 10, step=1, splits=Some(8))

    == Optimized Logical Plan ==
    Project [true AS foo#627]
    +- Range (0, 10, step=1, splits=Some(8))

    == Physical Plan ==
    *(1) Project [true AS foo#627]
    +- *(1) Range (0, 10, step=1, splits=8)
   */

  val resDf4: DataFrame =
    spark
      .range(0, 10)
      .select(($"id" > 0).alias("foo"))

  printPhysicalPlan(resDf4)
  /*
    *(1) Project [(id#629L > 0) AS foo#631]
    +- *(1) Range (0, 10, step=1, splits=8)
   */


  println("__Combine filters: ")
  /**
   * 6. Combine filters
   * .filter($"id" > 0) + .filter($"id" =!= 5) + .filter($"id" < 10) => Filter ((id#633L > 0) AND (NOT (id#633L = 5) AND (id#633L < 10)))
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
    *(1) Filter ((id#633L > 0) AND (NOT (id#633L = 5) AND (id#633L < 10)))
    +- *(1) Range (0, 10, step=1, splits=8)
   */

  resDf5.explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter ('id < 10)
    +- Filter NOT (id#633L = cast(5 as bigint))
       +- Project [id#633L]
          +- Filter (id#633L > cast(0 as bigint))
             +- Range (0, 10, step=1, splits=Some(8))

    == Analyzed Logical Plan ==
    id: bigint
    Filter (id#633L < cast(10 as bigint))
    +- Filter NOT (id#633L = cast(5 as bigint))
       +- Project [id#633L]
          +- Filter (id#633L > cast(0 as bigint))
             +- Range (0, 10, step=1, splits=Some(8))

    == Optimized Logical Plan ==
    Filter ((id#633L > 0) AND (NOT (id#633L = 5) AND (id#633L < 10)))
    +- Range (0, 10, step=1, splits=Some(8))

    == Physical Plan ==
    *(1) Filter ((id#633L > 0) AND (NOT (id#633L = 5) AND (id#633L < 10)))
    +- *(1) Range (0, 10, step=1, splits=8)
   */


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
