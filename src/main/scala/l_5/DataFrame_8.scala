package l_5

import l_5.DataFrame_5.printPhysicalPlan
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.lang

object DataFrame_8 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_5")
    .getOrCreate

  import spark.implicits._

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame = spark
    .read
    .options(csvOptions)
    .csv("src/main/resources/l_3/airport-codes.csv")

//  airportsDf
//    .write
//    .format("parquet")
//    .partitionBy("iso_country")
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/l_5/airports")

  val airportPartPqDf: DataFrame = spark
    .read
    .parquet("src/main/resources/l_5/airports")

  airportPartPqDf.printSchema
  println

  /** Column projection. */
  spark.time {
    val selectedDf: DataFrame = airportPartPqDf.select('ident)

    selectedDf.cache
    selectedDf.count
    selectedDf.unpersist

    printPhysicalPlan(selectedDf)
    /*
      *(1) Project [ident#65]
      +- *(1) ColumnarToRow
         // ReadSchema: struct<ident:string> - будет вычитана только колонка ident
         // В кэш будет помещена только эта колонка
         +- FileScan parquet [ident#65,iso_country#76] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string>
     */
  }  // 841 ms
  println()

  spark.time {
    val selectedDf: DataFrame = airportPartPqDf

    selectedDf.cache
    selectedDf.count
    selectedDf.unpersist

    printPhysicalPlan(selectedDf)
    /*
      *(1) ColumnarToRow
      +- FileScan parquet [ident#65,type#66,name#67,elevation_ft#68,continent#69,iso_region#70,municipality#71,gps_code#72,iata_code#73,local_code#74,coordinates#75,iso_country#76] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...
     */
  }  // 837 ms
  println()


  /**
   * Partition pruning.
   * .partitionBy("iso_country")
   *
   * PartitionFilters.
   */
  spark.time {
    val filteredDf: Dataset[Row] = airportPartPqDf.filter('iso_country === "RU")

    filteredDf.count

    printPhysicalPlan(filteredDf)
    /*
      *(1) ColumnarToRow
      // PartitionFilters: [isnotnull(iso_country#51), (iso_country#51 = RU)] - будет прочитан только каталог RU
      +- FileScan parquet [ident#65,type#66,name#67,elevation_ft#68,continent#69,iso_region#70,municipality#71,gps_code#72,iata_code#73,local_code#74,coordinates#75,iso_country#76] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [isnotnull(iso_country#76), (iso_country#76 = RU)], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...
     */
  }  // 119 ms
  println()

  spark.time {
    val filteredDf: Dataset[Row] = airportPartPqDf

    filteredDf.count

    printPhysicalPlan(filteredDf)
    /*
      *(1) ColumnarToRow
      // PartitionFilters: []
      +- FileScan parquet [ident#40,type#41,name#42,elevation_ft#43,continent#44,iso_region#45,municipality#46,gps_code#47,iata_code#48,local_code#49,coordinates#50,iso_country#51] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...
     */
  }  // 238 ms
  println()


  /**
   * Predicate pushdown.
   * PushedFilters.
   */
  spark.time {
    val filteredDf: Dataset[Row] = airportPartPqDf.filter('iso_region === "RU")

    filteredDf.count

    printPhysicalPlan(filteredDf)
    /*
      *(1) Filter (isnotnull(iso_region#70) AND (iso_region#70 = RU))
      +- *(1) ColumnarToRow
         // PushedFilters: [IsNotNull(iso_region), EqualTo(iso_region,RU)] - в паркете есть storage index, для каждой row group вычисляется min/max значение по колонке - фильтр будет применен на этом уровне
         +- FileScan parquet [ident#65,type#66,name#67,elevation_ft#68,continent#69,iso_region#70,municipality#71,gps_code#72,iata_code#73,local_code#74,coordinates#75,iso_country#76] Batched: true, DataFilters: [isnotnull(iso_region#70), (iso_region#70 = RU)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [IsNotNull(iso_region), EqualTo(iso_region,RU)], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_region:string,m...
     */
  }  // 340 ms
  println()


  /**
   * Simplify casts.
   * LongType.cast(LongType) => каста не будет.
   */
  val resDf1: DataFrame = spark
    .range(0, 10)
    .select('id.cast(LongType))

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
    Project [cast(id#529L as bigint) AS id#531L]
    +- Range (0, 10, step=1, splits=Some(8))

    == Optimized Logical Plan ==
    Range (0, 10, step=1, splits=Some(8))

    == Physical Plan ==
    *(1) Range (0, 10, step=1, splits=8)
   */

  /** LongType => IntegerType => LongType - оптимизация работать не будет. */
  val resDf2: DataFrame = spark
    .range(0, 10)
    .select('id.cast("int").cast("long"))

  printPhysicalPlan(resDf2)
  /*
    *(1) Project [cast(cast(id#533L as int) as bigint) AS id#536L]
    +- *(1) Range (0, 10, step=1, splits=8)
   */


  /**
   * Constant folding.
   * (lit(3) > lit(0) => true
   */
  val resDf3: Dataset[Row] = spark
    .range(0, 10)
    .select((lit(3) > lit(0)).alias("foo"))

  printPhysicalPlan(resDf3)
  /*
    *(1) Project [true AS foo#540]
    +- *(1) Range (0, 10, step=1, splits=8)
   */

  resDf3.explain(true)
  /*
    == Parsed Logical Plan ==
    Project [(3 > 0) AS foo#540]
    +- Range (0, 10, step=1, splits=Some(8))

    == Analyzed Logical Plan ==
    foo: boolean
    Project [(3 > 0) AS foo#540]
    +- Range (0, 10, step=1, splits=Some(8))

    == Optimized Logical Plan ==
    Project [true AS foo#540]
    +- Range (0, 10, step=1, splits=Some(8))

    == Physical Plan ==
    *(1) Project [true AS foo#540]
    +- *(1) Range (0, 10, step=1, splits=8)
   */

  val resDf4: DataFrame = spark
    .range(0, 10)
    .select(('id > 0).alias("foo"))

  printPhysicalPlan(resDf4)
  /*
    *(1) Project [(id#542L > 0) AS foo#544]
    +- *(1) Range (0, 10, step=1, splits=8)
   */


  /**
   * Combine filters.
   * .filter('id > 0) + .filter('id =!= 5) + .filter('id < 10) => Filter ((id#546L > 0) AND (NOT (id#546L = 5) AND (id#546L < 10)))
   */
  val resDf5: Dataset[Row] = spark
    .range(0, 10)
    .filter('id > 0)
    /** Проекция не мешает объединению фильтров. */
    .select(col("*"))
    .filter('id =!= 5)
    .filter('id < 10)

  printPhysicalPlan(resDf5)
  /*
    *(1) Filter ((id#546L > 0) AND (NOT (id#546L = 5) AND (id#546L < 10)))
    +- *(1) Range (0, 10, step=1, splits=8)
   */

  resDf5.explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter ('id < 10)
    +- Filter NOT (id#546L = cast(5 as bigint))
       +- Project [id#546L]
          +- Filter (id#546L > cast(0 as bigint))
             +- Range (0, 10, step=1, splits=Some(8))

    == Analyzed Logical Plan ==
    id: bigint
    Filter (id#546L < cast(10 as bigint))
    +- Filter NOT (id#546L = cast(5 as bigint))
       +- Project [id#546L]
          +- Filter (id#546L > cast(0 as bigint))
             +- Range (0, 10, step=1, splits=Some(8))

    == Optimized Logical Plan ==
    Filter ((id#546L > 0) AND (NOT (id#546L = 5) AND (id#546L < 10)))
    +- Range (0, 10, step=1, splits=Some(8))

    == Physical Plan ==
    *(1) Filter ((id#546L > 0) AND (NOT (id#546L = 5) AND (id#546L < 10)))
    +- *(1) Range (0, 10, step=1, splits=8)
   */

  Thread.sleep(1000000)
}
