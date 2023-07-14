/** т.к. private[sql] def internalCreateDataFrame */
package org.apache.spark.sql

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Random

class MyDataSourceProvider extends StreamSourceProvider with Logging {
  log.info(s"${this.logName} has been created")

  /** метод sourceSchema - вызывается первым */
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]
                           ): (String, StructType) = {

    log.info("sourceSchema call")

    schema match {
      case Some(s) => providerName -> s
      case None => throw new UnsupportedOperationException("Schema must be defined!")
    }
  }

  /** метод sourceSchema - вызывается вторым */
  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]
                           ): Source = {

    log.info("createSource call")

    schema match {
      case Some(s) => new MyDataSource(s)
      case None => throw new UnsupportedOperationException("Schema must be defined!")
    }
  }
}

class MyDataSource(sourceSchema: StructType) extends Source with Logging {
  log.info(s"${this.logName} has been created")

  val spark: SparkSession = SparkSession.active
  val sc: SparkContext = spark.sparkContext
  var batchCounter: Int = 0
  override def schema: StructType = sourceSchema

  /**
   * getOffset - вызывается драйвером перед вычиткой каждого микробатча
   *
   * если метод возвращает None - стрим пуст (в нем никогда не было данных)
   * если метод возвращает Some(value) и value != предыдущему оффсету - будет вызван getBatch(предыдущий оффсет, value)
   *
   * при сравнении оффсетов вызывается метод equals
   * если строки currentOffset.json() и previousOffset.json() не равны => в стриме есть новые данные => будет вызван getBatch
   */
  override def getOffset: Option[Offset] = {
    log.info("getOffset call")

//    None
//    Some(MyOffset(0))
    val offset: Some[MyOffset] = Some(MyOffset(batchCounter))
    log.info(s"New offset is $offset")

    offset
  }

  /** getBatch - получает данные для микробатча */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    log.info(s"getBatch call: start: $start, end: $end")  // Some(MyOffset(0)) => start: None, end: 0

    val rddIRow: RDD[InternalRow] = new MyRdd(this.schema)
    val df: DataFrame = spark.internalCreateDataFrame(rddIRow, this.schema, isStreaming = true)

//    spark.range(10).toDF()

    batchCounter += 1
    df
  }

  override def stop(): Unit = {
    log.info("The stream has been stopped!")
  }
}

case class MyOffset(i: Int) extends Offset {
  override def json(): String = i.toString
}

/** В нашей реализации sourceSchema - не нужна, в реальной жизни - понадобится */
class MyRdd(sourceSchema: StructType) extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) {
  /** compute - вызывается на action */
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val myPartition: MyPartition = split.asInstanceOf[MyPartition]
    val idx: Int = myPartition.index

    val rnd: Random.type = scala.util.Random

    /** !!! Алгоритм чтения данных из источника */
    new Iterator[InternalRow] {
      var i: Int = 0
      val last: Int = 2

      override def hasNext: Boolean = i <= last

      override def next(): InternalRow = {
        /**
         * err - java.lang.ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 1
         * - несоответствие количества элементов в InternalRow и заданной схеме
         */
//        val data: Seq[Int] = List(rnd.nextInt())

        /**
         * err - java.lang.ClassCastException: class java.lang.Integer cannot be cast to class org.apache.spark.unsafe.types.UTF8String
         * - несоответствие типов элементов в InternalRow и заданной схеме
         */
//        val data: Seq[Int] = List(rnd.nextInt(), rnd.nextInt())

        /**
         * err - java.lang.ClassCastException: class java.lang.String cannot be cast to class org.apache.spark.unsafe.types.UTF8String
         * - String автоматически не конвертируется в UTF8String, который используется в датафреймах
         */
//        val data: Seq[Any] = List(rnd.nextInt(), rnd.nextInt().toString)

        val data: Seq[Any] = List(rnd.nextInt(), UTF8String.fromString(rnd.nextInt().toString))
        val iRow: InternalRow = InternalRow.fromSeq(data)
        i += 1

        iRow
      }
    }


  }

  /** getPartitions - формирует набор партиций, вызывается на драйвере */
  override protected def getPartitions: Array[Partition] = {
    Array(
      MyPartition(0),
      MyPartition(1)
    )
  }
}

/** Партиция - некая метаинформация, необходимая для чтения данных */
case class MyPartition(index: Int) extends Partition