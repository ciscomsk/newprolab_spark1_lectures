package l_9

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RateTest extends App {
  val spark: SparkSession =
    SparkSession
    .builder()
    .master("local[8]")
    .appName("rate_test")
    .getOrCreate()

  val rateStreamDf: DataFrame =
    spark
      .readStream
      .format("rate")
      .load()

  val sink: DataStreamWriter[Row] =
    rateStreamDf
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"src/main/resources/l_9/chk/rate_test")
      .foreachBatch { (df: DataFrame, batchId: Long) =>
        val count: Long = df.count()
        println(s"Count=$count")
        println(s"BatchId=$batchId")

        df.show(1000, truncate = false)
      }

  sink.start()


  /** 3 - 1, 10 - 2, 10 - 3, 10 - 4 */
  /** 62 - 5 */
  /** 101 - 6 */
  /** 65 - 7, 2 - 8 */
  /** 929 - 9, 2 - 10 */
  Thread.sleep(1000000)

  spark.stop()
}
