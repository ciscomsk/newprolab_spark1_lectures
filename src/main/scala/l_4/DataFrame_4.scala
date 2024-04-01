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
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("DataFrame_4")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  import spark.implicits._

  /** Built-in functions */
  val df: Dataset[lang.Long] = spark.range(0, 10)
  val newColFunc: Column = pmod(col("id"), lit(2))
  // ==
  val newColExpr: Column = expr("pmod(id, 2)")
  println()

  df
    .withColumn("pmod", newColFunc)
    .show()

  df
    .withColumn("pmod", newColExpr)
    .show()

  println()


  /**
   * User-defined functions
   * при написании udf можно использовать монады - Option[T]/Try[T]
   *
   * при необходимости работать с DB/IO в udf - @transient lazy val pattern
   */
  val plusOne: UserDefinedFunction = udf { (value: Long) => value + 1 }

  df
    .withColumn("idPlusOne", plusOne(col("id")))
    .show(10, truncate = false)

  val hostname: UserDefinedFunction = udf { () => InetAddress.getLocalHost.getHostAddress }

  df
    .withColumn("hostname", hostname())
    .show(10, truncate = false)

  /** !!! None => null в DF */
  val divideTwoBy: UserDefinedFunction = udf { (inputValue: Long) => Try(2L / inputValue).toOption }

  val resultDf: DataFrame = df.withColumn("divideTwoBy", divideTwoBy(col("id")))
  resultDf.printSchema()
  /*
    root
     |-- id: long (nullable = false)
     |-- divideTwoBy: long (nullable = true)
   */
  resultDf.show(10, truncate = false)


  /** Joins */
  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

  val aggTypeCountryDf: DataFrame =
    airportsDf
      .groupBy($"type", $"iso_country")
      .agg(count("*").alias("cnt_country_type"))

  println("aggTypeCountryDf: ")
  aggTypeCountryDf.show(5, truncate = false)

  val aggCountryDf: DataFrame =
    airportsDf
      .groupBy($"iso_country")
      .agg(count("*").alias("cnt_country"))

  println("aggCountryDf: ")
  aggCountryDf.show(5, truncate = false)

  val innerJoinDf: DataFrame =
    aggTypeCountryDf
      .join(aggCountryDf, Seq("iso_country"), "inner") // inner join - default
      .select(
        $"iso_country",
        $"type",
        round(lit(100) * $"cnt_country_type" / $"cnt_country", 2).alias("percent")
      )

  println("innerJoinDf: ")
  innerJoinDf.show(5, truncate = false)

  val leftJoinDf: DataFrame = airportsDf.join(innerJoinDf, Seq("iso_country", "type"), "left")

  println("leftJoinDf: ")
  leftJoinDf
    .select(
      $"ident",
      $"iso_country",
      $"type",
      $"percent"
    )
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

  val joinCondition: Column =
    col("left_id") === col("right_id") and col("left_foo") === col("right_foo")
  // ==
  val joinConditionExpr: Column =
    expr("left.id = right.id and left.foo = right.foo")

  val leftDf: DataFrame = spark.range(10).withColumn("foo", lit("foo"))
  leftDf.printSchema()
  /*
    root
     |-- id: long (nullable = false)
     |-- foo: string (nullable = false)
   */
  leftDf.show()

  val rightDf: DataFrame = spark.range(10).withColumn("foo", lit("foo"))

  leftDf.as("left")
    .join(rightDf.as("right"), joinConditionExpr, "inner")
//    .drop($"right.id") // v1
    .select(
      leftDf("id").as("left_id"),
      leftDf("foo").as("left_foo"),
      rightDf("foo").as("right_foo")
    ) // v2
    .show()


  /** Window functions */
  val window: WindowSpec = Window.partitionBy("a", "b").orderBy("a")

  val windowCountry: WindowSpec = Window.partitionBy("iso_country")
  val windowTypeCountry: WindowSpec = Window.partitionBy("iso_country", "type")

  val res2Df: DataFrame =
    airportsDf
      .withColumn("cnt_country", count("*").over(windowCountry))
      .withColumn("cnt_country_type", count("*").over(windowTypeCountry))
      .withColumn("percent", round(lit(100) * $"cnt_country_type" / $"cnt_country", 2))

    res2Df.explain()
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Project [ident#90, type#91, name#92, elevation_ft#93, continent#94, iso_country#95, iso_region#96, municipality#97, gps_code#98, iata_code#99, local_code#100, coordinates#101, cnt_country#325L, cnt_country_type#341L, round((cast((100 * cnt_country_type#341L) as double) / cast(cnt_country#325L as double)), 2) AS percent#356]
         +- Window [count(1) windowspecdefinition(iso_country#95, type#91, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS cnt_country_type#341L], [iso_country#95, type#91]
            +- Sort [iso_country#95 ASC NULLS FIRST, type#91 ASC NULLS FIRST], false, 0
               +- Window [count(1) windowspecdefinition(iso_country#95, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS cnt_country#325L], [iso_country#95]
                  +- Sort [iso_country#95 ASC NULLS FIRST], false, 0
                     +- Exchange hashpartitioning(iso_country#95, 200), ENSURE_REQUIREMENTS, [plan_id=751]
                        +- FileScan csv [ident#90,type#91,name#92,elevation_ft#93,continent#94,iso_country#95,iso_region#96,municipality#97,gps_code#98,iata_code#99,local_code#100,coordinates#101] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
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
    +- Project [rn#399, ident#90]
       +- Window [row_number() windowspecdefinition(ident#90 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#399], [ident#90 ASC NULLS FIRST]
          +- Sort [ident#90 ASC NULLS FIRST], false, 0
             // !!! Exchange SinglePartition - все данные перемещаются в 1 партицию
             +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=888]
                +- FileScan csv [ident#90] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string>
   */

  /**
   * колонки оторваны от реальных данных
   * привязка происходит на этапе формирования физического плана/кодогенерации
   * до этого имеют статус - unresolved attribute
   */
  val cntCountry: Column = count("*").over(windowCountry).alias("cnt_country")
  val cntCountryType: Column = count("*").over(windowTypeCountry).alias("cnt_country_type")
  val percent: Column = round(lit(100) * cntCountryType / cntCountry).alias("percent")

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


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
