package l_4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions.{col, count, expr, lit, pmod, round, row_number, udf}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import java.lang
import java.net.InetAddress
import scala.util.{Failure, Success, Try}

object DataFrame_4 extends App {
  // не работает в Spark 3.3.0
//  Logger
//    .getLogger("org")
//    .setLevel(Level.OFF)

  val spark =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("DataFrame_4")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  /** Built-in functions */
  val df: Dataset[lang.Long] = spark.range(0, 10)
  val newColFunc: Column = pmod(col("id"), lit(2))
  // ==
  val newColExpr: Column = expr("pmod(id, 2)")

  df
    .withColumn("pmod", newColFunc)
    .show()

  df
    .withColumn("pmod", newColExpr)
    .show()

  println()


  /**
   * User-defined functions
   * При необходимости взаимодействия с бд в udf - @transient lazy val pattern
   * При написании udf можно использовать монады - Try[T]/Option[T]
   */
  val plusOne: UserDefinedFunction = udf { (value: Long) => value + 1 }

  df
    .withColumn("idPlusOne", plusOne(col("id")))
    .show(10, truncate = false)

  val hostname: UserDefinedFunction = udf { () => InetAddress.getLocalHost.getHostAddress }

  df
    .withColumn("hostname", hostname())
    .show(10, truncate = false)

  val divideToBy: UserDefinedFunction = udf { (inputValue: Long) => Try(2L / inputValue).toOption }
  val result: DataFrame = df.withColumn("divideTwoBy", divideToBy(col("id")))
  result.printSchema()
  result.show(10, truncate = false)


  /** Joins */
  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

  val aggTypeCountry: DataFrame =
    airportsDf
      .groupBy($"type", $"iso_country")
      .agg(count("*").alias("cnt_country_type"))

  println("aggTypeCountry: ")
  aggTypeCountry.show(5, truncate = false)

  val aggCountry: DataFrame =
    airportsDf
      .groupBy($"iso_country")
      .agg(count("*").alias("cnt_country"))

  println("aggCountry: ")
  aggCountry.show(5, truncate = false)

  val innerJoinDf: DataFrame =
    aggTypeCountry
      .join(aggCountry, Seq("iso_country"), "inner") // inner == default
      .select(
        $"iso_country",
        $"type",
        round(lit(100) * $"cnt_country_type" / $"cnt_country", 2).alias("percent")
      )

  println("percentDf: ")
  innerJoinDf.show(5, truncate = false)

  val leftJoinDf: DataFrame = airportsDf.join(innerJoinDf, Seq("iso_country", "type"), "left")

  println("resDf: ")
  leftJoinDf
    .select($"ident", $"iso_country", $"type", $"percent")
    /** sample(0.2) - выборка 20% значений из разных партиций */
    .sample(0.2)
    .show(20, truncate = false)

  Try {
    spark
      .range(10)
      .select(lit(0).alias("id"), lit(1).alias("id"), lit("a").alias("id"))
      .select("id")
  } match {
    case Success(df) => println(df.show())
    case Failure(ex) => println(ex)
  }

  println()

  val commonJoinCondition: Column =
    col("left_a") === col("right_a") and col("left_b") === col("right_b")

  val joinConditionExpr: Column =
    expr("left.id = right.id and left.foo = right.foo")

  val left: DataFrame = spark.range(10).withColumn("foo", lit("foo"))

  left.printSchema()
  left.show()

  val right: DataFrame = spark.range(10).withColumn("foo", lit("foo"))

  left.as("left")
    .join(right.as("right"), joinConditionExpr, "inner")
    .drop($"right.id")  // v1
    .select($"id", left("foo").as("left_foo"))  // v2
    .show()


  /** Window functions */
  val window: WindowSpec = Window.partitionBy("a", "b").orderBy("a")

  val windowCountry: WindowSpec = Window.partitionBy("iso_country")
  val windowTypeCountry: WindowSpec = Window.partitionBy("type", "iso_country")

  val res2Df: DataFrame =
    airportsDf
      .withColumn("cnt_country", count("*").over(windowCountry))
      .withColumn("cnt_country_type", count("*").over(windowTypeCountry))
      .withColumn("percent", round(lit(100) * $"cnt_country_type" / $"cnt_country", 2))

    res2Df.explain()
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Project [ident#90, type#91, name#92, elevation_ft#93, continent#94, iso_country#95, iso_region#96, municipality#97, gps_code#98, iata_code#99, local_code#100, coordinates#101, cnt_country#313L, cnt_country_type#329L, round((cast((100 * cnt_country_type#329L) as double) / cast(cnt_country#313L as double)), 2) AS percent#344]
         +- Window [count(1) windowspecdefinition(type#91, iso_country#95, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS cnt_country_type#329L], [type#91, iso_country#95]
            +- Sort [type#91 ASC NULLS FIRST, iso_country#95 ASC NULLS FIRST], false, 0
               +- Window [count(1) windowspecdefinition(iso_country#95, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS cnt_country#313L], [iso_country#95]
                  +- Sort [iso_country#95 ASC NULLS FIRST], false, 0
                     +- Exchange hashpartitioning(iso_country#95, 200), ENSURE_REQUIREMENTS, [id=#752]
                        +- FileScan csv [ident#90,type#91,name#92,elevation_ft#93,continent#94,iso_country#95,iso_region#96,municipality#97,gps_code#98,iata_code#99,local_code#100,coordinates#101] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn_2/courses/spark/newprolab/spark_1/_repos/lectur..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
     */

  res2Df
    .select(
      $"ident",
      $"iso_country",
      $"type",
      $"percent"
    )
    .sample(0.2)
    .show(20, truncate = false)

  val rowNumberDf: DataFrame =
    airportsDf
      .withColumn("rn", row_number().over(Window.partitionBy().orderBy("ident")))
      .select("rn", "ident")

  rowNumberDf.show()
  rowNumberDf.explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [rn#387, ident#90]
       +- Window [row_number() windowspecdefinition(ident#90 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#387], [ident#90 ASC NULLS FIRST]
          +- Sort [ident#90 ASC NULLS FIRST], false, 0
             // !!! Exchange SinglePartition - все сливается в 1 партицию
             +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#856]
                +- FileScan csv [ident#90] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn_2/courses/spark/newprolab/spark_1/_repos/lectur..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string>

   */

  /**
   * Колонки оторваны от данных
   * Привязка происходит на этапе формирования физического плана
   */

  val cntCountry: Column = count("*").over(windowCountry).alias("cnt_country")
  val cntCountryType: Column = count("*").over(windowTypeCountry).alias("cnt_country_type")
  val percent: Column = round(lit(100) *  cntCountryType / cntCountry).alias("percent")

  val res3Df: DataFrame = airportsDf.select($"*", percent)

  res3Df
    .select(
      $"ident",
      $"iso_country",
      $"type",
      $"percent"
    )
    .sample(0.2)
    .show(20, truncate = false)

  spark.stop()
}
