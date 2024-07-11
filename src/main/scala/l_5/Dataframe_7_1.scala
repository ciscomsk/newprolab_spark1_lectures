package l_5

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

object Dataframe_7_1 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

  import spark.implicits._

//  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")
  val csvOptions: Map[String, String] = Map("header" -> "true")

  val ddlSchema: String =
    "ident STRING,type STRING,name STRING,elevation_ft INT,continent STRING,iso_country STRING,iso_region STRING,municipality STRING,gps_code STRING,iata_code STRING,local_code STRING,coordinates STRING"

  val schema: StructType = StructType.fromDDL(ddlSchema)

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .schema(schema)
      .csv("src/main/resources/l_3/airport-codes.csv")

  println(airportsDf.schema.toDDL)
//  ident STRING,type STRING,name STRING,elevation_ft INT,continent STRING,iso_country STRING,iso_region STRING,municipality STRING,gps_code STRING,iata_code STRING,local_code STRING,coordinates STRING
  println()


  /**
   * Снижение объема shuffle
   *
   * предварительное репартицирование по ключам имеет смысл делать если после него идет
   * несколько операций требующих репартицирования по этим ключам (Exchange hashpartitioning)
   */
  spark.time {
    val leftDf: DataFrame = airportsDf

    val rightDf: DataFrame =
      airportsDf
        .groupBy($"type")
        .count()

    val joinedDf: DataFrame = leftDf.join(rightDf, Seq("type"))

    joinedDf.explain()
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Project [type#1, ident#0, name#2, elevation_ft#3, continent#4, iso_country#5, iso_region#6, municipality#7, gps_code#8, iata_code#9, local_code#10, coordinates#11, count#37L]
         +- SortMergeJoin [type#1], [type#42], Inner
            :- Sort [type#1 ASC NULLS FIRST], false, 0
            :  +- Exchange hashpartitioning(type#1, 200), ENSURE_REQUIREMENTS, [plan_id=39]
            :     +- Filter isnotnull(type#1)
            :        +- FileScan csv [ident#0,type#1,name#2,elevation_ft#3,continent#4,iso_country#5,iso_region#6,municipality#7,gps_code#8,iata_code#9,local_code#10,coordinates#11] Batched: false, DataFilters: [isnotnull(type#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [IsNotNull(type)], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
            +- Sort [type#42 ASC NULLS FIRST], false, 0
               +- HashAggregate(keys=[type#42], functions=[count(1)])
                  +- Exchange hashpartitioning(type#42, 200), ENSURE_REQUIREMENTS, [plan_id=35]
                     +- HashAggregate(keys=[type#42], functions=[partial_count(1)])
                        +- Filter isnotnull(type#42)
                           +- FileScan csv [type#42] Batched: false, DataFilters: [isnotnull(type#42)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [IsNotNull(type)], ReadSchema: struct<type:string>
     */

//    joinedDf.show(numRows = 1)

    /** план в SQL/DataFrame показан для joinedDf.count(), а не joinedDf */
    joinedDf.count()
  } // 2590 ms
  println()


  println(sc.uiWebUrl)
  Thread.sleep(1_000_000)

  spark.stop()
}
