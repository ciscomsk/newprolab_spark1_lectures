package playground

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

object RepartitionTest extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  println(spark.sparkContext.uiWebUrl)

  val df = spark.read.parquet("src/main/resources/l_6/airports-8.parquet")
  val dfCol = df.withColumn("zzz", lit("zzz"))
//  df.cache()
//  df.count()

//  val partDf = df.repartition(col("gps_code"), col("iso_country"))

//  val sortedDf = df.sort(col("gps_code"), col("iso_country"))
//  sortedDf.cache()
//  sortedDf.count()
//
//  sortedDf.dropDuplicates(Seq("gps_code", "iso_country")).show()
//  sortedDf.dropDuplicates(Seq("gps_code", "iso_country", "iso_region")).show()
//  sortedDf.dropDuplicates(Seq("gps_code", "iso_country", "local_code")).show()
//  sortedDf.dropDuplicates(Seq("gps_code", "iso_country", "local_code")).explain()

  def calcDf(df: DataFrame) = {
    println("1")
  }

  dfCol.repartition()
  dfCol.orderBy()
  dfCol.dropDuplicates()
  dfCol.distinct()

  calcDf(dfCol)

  dfCol.unpersist()

  Thread.sleep(1000000)
}
