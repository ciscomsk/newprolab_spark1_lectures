package l_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

object CheckpointS3 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.OFF)

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_7")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  sc.hadoopConfiguration.set("fs.s3a.access.key", "admin")
  sc.hadoopConfiguration.set("fs.s3a.secret.key", "adminsecretkey")
  sc.hadoopConfiguration.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
  sc.hadoopConfiguration.set("fs.s3a.path.style.access", "true")

  val rateStreamDf: DataFrame =
    spark
      .readStream
      .format("rate")
      .load()

  rateStreamDf
    .writeStream
    .format("parquet")
    .option("path", s"s3a://data/raw/")
    .option("checkpointLocation", s"s3a://data/checkpoint/")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()
    .awaitTermination(15000)

  println("done")

}
