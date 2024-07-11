package l_4

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object RepartitionTest extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("DataFrame_2")
      .getOrCreate()

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val schema: StructType =
    StructType(
      Array(
        StructField("ident", StringType),
        StructField("type", StringType),
        StructField("name", StringType),
        StructField("elevation_ft", IntegerType),
        StructField("continent", StringType),
        StructField("iso_country", StringType),
        StructField("iso_region", StringType),
        StructField("municipality", StringType),
        StructField("gps_code", StringType),
        StructField("iata_code", StringType),
        StructField("local_code", StringType),
        StructField("coordinates", StringType),
      )
    )

  val airportsDf: DataFrame =
    spark
      .read
      .schema(schema)
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

//  airportsDf.printSchema()

//  val repDf: Dataset[Row] = airportsDf.repartition(10)
  val repDf: Dataset[Row] = airportsDf.orderBy("iso_country")

  repDf.explain()

  println(spark.sparkContext.uiWebUrl)
  Thread.sleep(1_000_000)
}
