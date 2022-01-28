package l_4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions.{col, count, expr, lit, pmod, round, udf}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import java.lang
import java.net.InetAddress
import scala.util.Try

object DataFrame_4 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("DataFrame_4")
    .getOrCreate

  import spark.implicits._

  /** Built-in functions. */
  val df: Dataset[lang.Long] = spark.range(0, 10)

  val newColFunc: Column = pmod(col("id"), lit(2))

  df
    .withColumn("pmod", newColFunc)
    .show

  val newColExpr: Column = expr("""pmod(id, 2)""")

  df
    .withColumn("pmod", newColExpr)
    .show

  println()


  /** User-defined functions. */
  /** При необходимости взаимодействия с базой в udf - @transient lazy val pattern. */
  val plusOne: UserDefinedFunction = udf { (value: Long) => value + 1 }

  df
    .withColumn("idPlusOne", plusOne(col("id")))
    .show(10, truncate = false)

  val hostname: UserDefinedFunction = udf { () => InetAddress.getLocalHost.getHostAddress }

  df
    .withColumn("hostname", hostname())
    .show(10, truncate = false)

  /** Можно использовать монады Try[T]/Option[T] */
  val divideToBy: UserDefinedFunction = udf { (inputValue: Long) => Try(2L / inputValue).toOption }
  val result: DataFrame = df.withColumn("divideTwoBy", divideToBy(col("id")))
  result.printSchema
  result.show(10, truncate = false)


  /** Joins. */
  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame = spark
    .read
    .options(csvOptions)
    .csv("src/main/resources/l_3/airport-codes.csv")

  val aggTypeCountry: DataFrame = airportsDf
    .groupBy('type, 'iso_country)
    .agg(count("*").alias("cnt_country_type"))

  aggTypeCountry.show(5, truncate = false)

  val aggCountry: DataFrame = airportsDf
    .groupBy('iso_country)
    .agg(count("*").alias("cnt_country"))

  aggCountry.show(5, truncate = false)

  val percentDf: DataFrame = aggTypeCountry
    .join(aggCountry, Seq("iso_country"), "inner")  // inner == default
    .select(
      'iso_country,
      'type,
      round(lit(100) * 'cnt_country_type / 'cnt_country, 2).alias("percent")
    )

  percentDf.show(5, truncate = false)

  val resDf: DataFrame = airportsDf.join(percentDf, Seq("iso_country", "type"), "left")

  resDf
    .select('ident, 'iso_country, 'type, 'percent)
    /** sample(0.2) - выборка 20% значений из разных партиций. */
    .sample(0.2)
    .show(20, truncate = false)

  val commonJoinCondition: Column =
    col("left_a") === col("right_a") and col("left_b") === col("right_b")


  /** Window functions. */
  val windowCountry: WindowSpec = Window.partitionBy("iso_country")
  val windowTypeCountry: WindowSpec = Window.partitionBy("type", "iso_country")

  val res2Df: DataFrame = airportsDf
    .withColumn("cnt_country", count("*").over(windowCountry))
    .withColumn("cnt_country_type", count("*").over(windowTypeCountry))
    .withColumn("percent", round(lit(100) * 'cnt_country_type / 'cnt_country, 2))

  res2Df
    .select(
      'ident,
      'iso_country,
      'type,
      'percent
    )
    .sample(0.2)
    .show(20, truncate = false)

  /** Колонки оторваны от данных. Привязываются только на этапе формирования физического плана. */
  val cntCountry: Column = count("*").over(windowCountry).alias("cnt_country")
  val cntCountryType: Column = count("*").over(windowTypeCountry).alias("cnt_country_type")
  val percent: Column = round(lit(100) *  cntCountryType / cntCountry).alias("percent")

  val res3Df: DataFrame = airportsDf
    .select(
      '*,
      percent
    )

  res3Df
    .select(
      'ident,
      'iso_country,
      'type,
      'percent
    )
    .sample(0.2)
    .show(20, truncate = false)

  spark.stop
}
