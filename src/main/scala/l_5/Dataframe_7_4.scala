package l_5

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Dataframe_7_4 extends App {
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


  spark.time {
    /** repartition ~= persist(StorageLevel.DISK_ONLY) */
    val airportsRepDf: Dataset[Row] = airportsDf.repartition(200, col("type")) // cache
    /** !!! cache после repartition не имеет смысла */
    airportsRepDf.cache()

    val leftDf: Dataset[Row] = airportsRepDf

    val rightDf: DataFrame =
      airportsRepDf
        .groupBy($"type")
        .count()

    val joinedDf: DataFrame = leftDf.join(rightDf, Seq("type"))

    joinedDf.explain()
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Project [type#1, ident#0, name#2, elevation_ft#3, continent#4, iso_country#5, iso_region#6, municipality#7, gps_code#8, iata_code#9, local_code#10, coordinates#11, count#98L]
         +- SortMergeJoin [type#1], [type#102], Inner
            :- Sort [type#1 ASC NULLS FIRST], false, 0
            :  +- Exchange hashpartitioning(type#1, 200), ENSURE_REQUIREMENTS, [plan_id=39]
            :     +- Filter isnotnull(type#1)
            :        +- InMemoryTableScan [ident#0, type#1, name#2, elevation_ft#3, continent#4, iso_country#5, iso_region#6, municipality#7, gps_code#8, iata_code#9, local_code#10, coordinates#11], [isnotnull(type#1)]
            :              +- InMemoryRelation [ident#0, type#1, name#2, elevation_ft#3, continent#4, iso_country#5, iso_region#6, municipality#7, gps_code#8, iata_code#9, local_code#10, coordinates#11], StorageLevel(disk, memory, deserialized, 1 replicas)
            :                    +- AdaptiveSparkPlan isFinalPlan=false
            :                       +- Exchange hashpartitioning(type#1, 200), REPARTITION_BY_NUM, [plan_id=6]
            :                          +- FileScan csv [ident#0,type#1,name#2,elevation_ft#3,continent#4,iso_country#5,iso_region#6,municipality#7,gps_code#8,iata_code#9,local_code#10,coordinates#11] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
            +- Sort [type#102 ASC NULLS FIRST], false, 0
               +- HashAggregate(keys=[type#102], functions=[count(1)])
                  +- Exchange hashpartitioning(type#102, 200), ENSURE_REQUIREMENTS, [plan_id=35]
                     +- HashAggregate(keys=[type#102], functions=[partial_count(1)])
                        +- Filter isnotnull(type#102)
                           +- InMemoryTableScan [type#102], [isnotnull(type#102)]
                                 +- InMemoryRelation [ident#101, type#102, name#103, elevation_ft#104, continent#105, iso_country#106, iso_region#107, municipality#108, gps_code#109, iata_code#110, local_code#111, coordinates#112], StorageLevel(disk, memory, deserialized, 1 replicas)
                                       +- AdaptiveSparkPlan isFinalPlan=false
                                          +- Exchange hashpartitioning(type#1, 200), REPARTITION_BY_NUM, [plan_id=6]
                                             +- FileScan csv [ident#0,type#1,name#2,elevation_ft#3,continent#4,iso_country#5,iso_region#6,municipality#7,gps_code#8,iata_code#9,local_code#10,coordinates#11] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
     */

//    joinedDf.show(numRows = 1)

    /** план в SQL/DataFrame показан для joinedDf.count(), а не joinedDf */
    joinedDf.count()
    airportsRepDf.unpersist()
  } // 4613 ms
  println()


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
