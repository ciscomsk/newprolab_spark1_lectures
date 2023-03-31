package l_2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Spark extends App {
//   не работает в Spark 3.3.2
//  Logger
//    .getLogger("org")
//    .setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_2")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  spark
    .sql("select 1")
    .show()
}
