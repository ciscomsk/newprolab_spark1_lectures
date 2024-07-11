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

  /** sbt_shell -> testOnly l_10.WriterSpec */
  "Writer" should "write" in {
    val sq: StreamingQuery =
      df
        .writeStream
        .trigger(Trigger.ProcessingTime("10 seconds"))
//        .format("MyDataSinkProvider") // FQDN
        .format("l_10.MyDataSinkProvider")
        .option("foo", "bar") // add 1.1
        /** !!! для кастомного синка обязательно указание чекпоинта */
        .option("checkpointLocation", "src/main/resources/l_10/chk/MyDataSinkProvider") // add 2
//        .partitionBy("col1", "col2") // add 1.2
        .partitionBy("value")
        .outputMode(OutputMode.Append) // add 1.3
        .start()

    sq.awaitTermination(25000)
    sq.stop()

    /**
     * 1. org.apache.spark.SparkClassNotFoundException: [DATA_SOURCE_NOT_FOUND] Failed to find the data source: MyDataSinkProvider
     * -> 2. org.apache.spark.SparkUnsupportedOperationException: Data source l_10.MyDataSinkProvider does not support streamed writing
     * -> 3. org.apache.spark.sql.AnalysisException: Partition column col1 not found in existing columns (timestamp, value)
     * -> 4. scala.NotImplementedError: an implementation is missing
     * -> 5. org.apache.spark.sql.AnalysisException: checkpointLocation must be specified either through option("checkpointLocation", ...) or SparkSession.conf.set("spark.sql.streaming.checkpointLocation", ...)
     * -> 6. scala.NotImplementedError: an implementation is missing
     * -> 7. ERROR MicroBatchExecution: Query [id = f7a20aaa-97d9-4324-b738-359f166b12d5, runId = 4dbd164b-8a05-4aad-b4bd-777471c6df52] terminated with error
     *       org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start()
     */
  }
}
