package l_5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SaveModeSpec extends App {
  // не работает в Spark 3.3.1
//  Logger
//    .getLogger("org")
//    .setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
    .builder()
    .master("local[*]")
    .appName("l_5")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

//    airportsDf
//      .limit(10)
//      .write
//      .format("json")
//      .mode(SaveMode.Overwrite)
//      .partitionBy("iso_country")
//      .save("src/test/resources/l_5/airports")

  val diffFormats =
    spark
      .read
      .format("json")
      .load("src/test/resources/l_5/airports")

  diffFormats.show(numRows = 100)
}
