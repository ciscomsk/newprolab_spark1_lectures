package l_9

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{array, lit, shuffle}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object HelperExplainStreaming9 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_9")
      .getOrCreate()

  import spark.implicits._

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")
  println(sc.uiWebUrl)
  println()

  def airportsDf(): DataFrame = {
    /** !!! inferSchema добавляет 2 джобы по выводу схемы в даг */
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

    val testSchema: StructType =
      StructType.fromDDL("ident STRING,type STRING,name STRING,elevation_ft INT,continent STRING,iso_country STRING,iso_region STRING,municipality STRING,gps_code STRING,iata_code STRING,local_code STRING,coordinates STRING")

    val csvDf: DataFrame =
      spark
        .read
//        .schema(testSchema)
        .options(csvOptions)
        .csv("src/main/resources/l_3/airport-codes.csv")

    csvDf.explain()
    /*
      == Physical Plan ==
      FileScan csv [ident#21,type#22,name#23,elevation_ft#24,continent#25,iso_country#26,iso_region#27,municipality#28,gps_code#29,iata_code#30,local_code#31,coordinates#32] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
     */

    println(csvDf.schema.toDDL)
    csvDf
  }

  airportsDf()

  def getRandomIdent: Column = {
    val identsDs: Dataset[String] =
      airportsDf()
        .select($"ident")
        .limit(20)
        .distinct
        .as[String]

    identsDs.explain()
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- HashAggregate(keys=[ident#21], functions=[])
         +- HashAggregate(keys=[ident#21], functions=[])
            +- GlobalLimit 20, 0
               +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=41]
                  +- LocalLimit 20
                     +- FileScan csv [ident#21] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string>
     */

    val idents: Array[String] = identsDs.collect()

    val columnArray: Array[Column] = idents.map(lit)
    val sparkArray: Column = array(columnArray: _*)
    val shuffledArray: Column = shuffle(sparkArray)

    shuffledArray(0)
  }

  def createSink(chkName: String, df: DataFrame)(batchFunc: (DataFrame, Long) => Unit): DataStreamWriter[Row] = {
    df
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_9/chk/$chkName")
      .foreachBatch(batchFunc)
  }

  val myStreamDf: DataFrame =
    spark
      .readStream
      .format("rate")
      .load()
      .withColumn("ident", getRandomIdent)

  myStreamDf.explain()
  /*
    == Physical Plan ==
    *(1) Project [timestamp#0, value#1L, shuffle([00A,00AA,00AK,00AL,00AR,00AS,00AZ,00CA,00CL,00CN,00CO,00FA,00FD,00FL,00GA,00GE,00HI,00ID,00IG,00II], Some(-512346443606943889))[0] AS ident#51]
    +- StreamingRelation rate, [timestamp#0, value#1L]
   */


  Thread.sleep(100000)
}
