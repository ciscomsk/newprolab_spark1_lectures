package l_2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Spark extends App {
  val log: Logger = Logger.getLogger("org")
  log.setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("l_2")
    .getOrCreate

  spark
    .sql("select 1")
    .show
}
