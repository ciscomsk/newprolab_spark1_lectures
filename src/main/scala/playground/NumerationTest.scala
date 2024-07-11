package playground

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.lang

object NumerationTest extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  import spark.implicits._

  val df: Dataset[lang.Long] = spark.range(100)

  df
    .repartition(10)
    .mapPartitions { part =>
      val seq = part.toSeq
      println(seq.length)
      seq.iterator
    }
    .show(100, truncate = false)

}
