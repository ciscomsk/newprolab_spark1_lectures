package playground

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object NullTest extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  import spark.implicits._

  val data: Seq[(java.lang.Long, java.lang.Long)] =
    Seq(
      (1675167853L, null)
    )

  val df: DataFrame = data.toDF("l1", "l2")

  df
    .withColumn("l1+l2", col("l1") + col("l2"))
    .show()
}
