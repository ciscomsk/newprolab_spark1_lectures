package l_11

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, rand, round}

import java.lang

object OrderBy extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  spark.conf.set("spark.sql.adaptive.enabled", "false")

  val df1: DataFrame = spark.range(0, 1000).withColumn("value", round(rand() * 2, 0))
  val df2: DataFrame = spark.range(0, 1000).withColumn("value", round(rand() * 2, 0))
  val testDf = df1.union(df2).cache()
//  testDf.show()

  /**  */
//  testDf
//    .repartition(1, col("id"), col("value"))
//    .write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/l_11/sort/repartition")

  /**  */
//  val orderedRes =
//    testDf
//      .repartition(2)
//      .orderBy("id", "value")
//
//  orderedRes.explain()
//
//  orderedRes
//    .write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/l_11/sort/order_by")

//  val readDf = spark.read.parquet("src/main/resources/l_11/sort/order_by")

  /**  */
  val sortWithinPartitionsRes =
    testDf
      .repartition(2)
      .sortWithinPartitions("id", "value")

  sortWithinPartitionsRes.explain()

  sortWithinPartitionsRes
    .write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/l_11/sort/sort_within_partitions")

  val readDf =
    spark
      .read
      .parquet("src/main/resources/l_11/sort/sort_within_partitions")
      .orderBy("id", "value")

  readDf
    .write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/l_11/sort/saved_again")


}
