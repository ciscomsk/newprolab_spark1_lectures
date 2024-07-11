package org.apache.spark.sql

import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.StructType

class StreamSourceEx extends StreamSourceProvider {
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]
                           ): (String, StructType) = ???

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]
                           ): Source = ???
}

class SourceEx extends Source {
  override def schema: StructType = ???

  override def getOffset: Option[Offset] = ???

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = ???

  override def stop(): Unit = ???
}

class OffsetEx extends Offset {
  override def json(): String = ???
}

class RddEx(sc: SparkContext, dep: Seq[Dependency[_]]) extends RDD[InternalRow](sc, dep) {
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = ???

  override protected def getPartitions: Array[Partition] = ???
}

class PartitionEx extends Partition {
  override def index: Int = ???
}