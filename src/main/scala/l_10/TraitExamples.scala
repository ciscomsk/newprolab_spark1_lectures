package l_10

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode

class StreamSinkEx extends StreamSinkProvider {
  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String],
                           outputMode: OutputMode
                         ): Sink = ???
}

class SinkEx extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = ???
}
