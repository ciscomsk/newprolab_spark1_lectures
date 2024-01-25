package l_8

import org.apache.spark.sql.functions.{col, current_timestamp}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Dataset, SparkSession}

import java.lang

object CastLong extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  val df: Dataset[lang.Long] = spark.range(1)

  df
    .withColumn("ts", current_timestamp)
    .withColumn("timestamp.cast(LongType)", col("ts").cast(LongType))
    .withColumn("timestamp.cast(LongType) / 60", col("ts").cast(LongType) / 60) // == 28332303.066666667
    .withColumn("(timestamp.cast(LongType) / 60).cast(LongType)", (col("ts").cast(LongType) / 60).cast(LongType))
    .withColumn("(timestamp.cast(LongType) / 60).cast(LongType) * 60", (col("ts").cast(LongType) / 60).cast(LongType) * 60)
    .withColumn("((timestamp.cast(LongType) / 60).cast(LongType) * 60).cast(TimestampType)", ((col("ts").cast(LongType) / 60).cast(LongType) * 60).cast(TimestampType))
    .show(truncate = false)

}
