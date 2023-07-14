package l_10

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class WriterSpec extends AnyFlatSpec with should.Matchers with SparkSupport {

  val df: DataFrame =
    spark
      .readStream
      .format("rate")
      .load()

  /** sbt_shell => testOnly l_10.WriterSpec */
  "Writer" should "write" in {
    val sq: StreamingQuery =
      df
        .writeStream
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .format("l_10.MyDataSinkProvider")  // FQDN - was MyDataSinkProvider
        .option("foo", "bar")
        /** !!! Для кастомного синка обязательно указание чекпоинта */
        .option("checkpointLocation", "src/main/resources/l_10/chk/MyDataSinkProvider")
        .partitionBy("value") // was ("col1", "col2")
        .outputMode(OutputMode.Append)
        .start()

    sq.awaitTermination(25000)
    sq.stop()

    /**
     * 1. org.apache.spark.SparkClassNotFoundException: [DATA_SOURCE_NOT_FOUND] Failed to find the data source: l_10.MyDataSinkProvider
     * Cause: java.lang.ClassNotFoundException: l_10.MyDataSinkProvider.DefaultSource
     * => 2. org.apache.spark.SparkUnsupportedOperationException: Data source l_10.MyDataSinkProvider does not support streamed writing
     * => 3. org.apache.spark.sql.AnalysisException: Partition column col1 not found in existing columns (timestamp, value)
     * => 4. scala.NotImplementedError: an implementation is missing
     * => 5. org.apache.spark.sql.AnalysisException: checkpointLocation must be specified either through option("checkpointLocation", ...) or SparkSession.conf.set("spark.sql.streaming.checkpointLocation", ...)
     * => 6. scala.NotImplementedError: an implementation is missing
     * => 7. org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start();
     */
  }
}
