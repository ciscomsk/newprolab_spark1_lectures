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

  /** sourceSchema - вызывается первой. */
  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],  // то, что будет передано в readStream.schema
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {

    log.info("sourceSchema call")

    schema match {
      case Some(schema) =>
        providerName -> schema

      case None =>
        throw new UnsupportedOperationException("Schema must be defined")
    }
  }

  /** createSource - вызывается второй. */
  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {

    log.info("createSource call")

    schema match {
      case Some(schema) =>
        new MyDataSource(schema)

      case None =>
        throw new UnsupportedOperationException("Schema must be defined")
    }
  }
}

class MyDataSource(sourceSchema: StructType) extends Source with Logging {
  log.info(s"${this.logName} has been created")

  val spark: SparkSession = SparkSession.active
  val sc: SparkContext = spark.sparkContext

  var counter: Int = 0

  override def schema: StructType = sourceSchema

  /**
   * getOffset - вызывается драйвером перед вычиткой каждого микробатча.
   *
   * Если функция возвращает None - стрим пуст. В нем никогда не было данных.
   * Если функция возвращает Some(value) != предыдущему оффсету - будет вызван getBatch(предыдущий оффсет, value).
   */
  override def getOffset: Option[Offset] = {
    log.info("getOffset call")

//    None
//    Some(MyOffset(0))
    val offset: Option[MyOffset] = Some(MyOffset(counter))
    log.info(s"New offset is: $offset")

    offset
  }

  /** getBatch - получает данные для микробатча */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    log.info(s"getBatch call: start: $start, end: $end")  // Some(MyOffset(0)) => start: None, end: 0

    /** err - DataFrame returned by getBatch from org.apache.spark.sql.MyDataSource@7bf50ab2 did not have isStreaming=true */
//    spark.range(10).toDF()

    val iRowRdd: RDD[InternalRow] = new MyRdd

    /** package org.apache.spark.sql */
    /*
      /** Creates a `DataFrame` from an `RDD[InternalRow]`. */
      private[sql] def internalCreateDataFrame(
          catalystRows: RDD[InternalRow],
          schema: StructType,
          isStreaming: Boolean = false): DataFrame = {
     */

    counter += 1

    spark.internalCreateDataFrame(iRowRdd, this.schema, isStreaming = true)
  }

  override def stop(): Unit = {
    log.info("The stream has been stopped!")
  }
}

/**
 * При сравнении оффсетов вызывается метод equals.
 * Если строки (currentOffset.json() != previousOffset.json()) не равны => в стриме есть новые данные => будет вызван getBatch.
 */

/** package org.apache.spark.sql.connector.read.streaming */
/*
  /**
   * An abstract representation of progress through a {@link MicroBatchStream} or
   * {@link ContinuousStream}.
   * <p>
   * During execution, offsets provided by the data source implementation will be logged and used as
   * restart checkpoints. Each source should provide an offset implementation which the source can use
   * to reconstruct a position in the stream up to which data has been seen/processed.
   *
   * @since 3.0.0
   */
  @Evolving
  public abstract class Offset {
    /**
     * A JSON-serialized representation of an Offset that is
     * used for saving offsets to the offset log.
     * <p>
     * Note: We assume that equivalent/equal offsets serialize to
     * identical JSON strings.
     *
     * @return JSON string encoding
     */
    public abstract String json();

    /**
     * Equality based on JSON string representation. We leverage the
     * JSON representation for normalization between the Offset's
     * in deserialized and serialized representations.
     */
    @Override
      public boolean equals(Object obj) {
          if (obj instanceof Offset) {
              return this.json().equals(((Offset) obj).json());
          } else {
              return false;
          }
      }
      ...
 */

case class MyOffset(i: Int) extends Offset {
  override def json(): String = s"$i"
}

/** package org.apache.spark.rdd */
/*
  abstract class RDD[T: ClassTag](
      @transient private var _sc: SparkContext,
      @transient private var deps: Seq[Dependency[_]]
    ) extends Serializable with Logging {
    ...
    /**
     * :: DeveloperApi ::
     * Implemented by subclasses to compute a given partition.
     */
    @DeveloperApi
    def compute(split: Partition, context: TaskContext): Iterator[T]  // вызывается на action

    /**
     * Implemented by subclasses to return the set of partitions in this RDD. This method will only
     * be called once, so it is safe to implement a time-consuming computation in it.
     *
     * The partitions in this array must satisfy the following property:
     *   `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
     */
    protected def getPartitions: Array[Partition] // формирует набор партиций - вызывается на драйвере
    ...
 */
class MyRdd extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val myPartition: MyPartition = split.asInstanceOf[MyPartition]
    val idx: Int = myPartition.index

    val rnd: Random.type = scala.util.Random

    new Iterator[InternalRow] {
      var i: Int = 0
      val last: Int = 4

      override def hasNext: Boolean = i <= last

      override def next(): InternalRow = {
        // err - java.lang.ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 1 - несоответствие количества элементов в InternalRow и заданной схеме
//        val data: List[Any] = rnd.nextInt() :: Nil

        // err - java.lang.ClassCastException: class java.lang.String cannot be cast to class org.apache.spark.unsafe.types.UTF8String (java.lang.String is in module java.base of loader 'bootstrap'; org.apache.spark.unsafe.types.UTF8String is in unnamed module of loader 'app') - несоответствие типов элементов в InternalRow и заданной схеме
//        val data: List[Any] = List(rnd.nextInt(), rnd.nextInt().toString)

        val data: List[Any] = List(rnd.nextInt(), UTF8String.fromString(rnd.nextInt.toString))
        val iRow: InternalRow = InternalRow.fromSeq(data)
        i += 1

        iRow
      }
    }
  }

  override protected def getPartitions: Array[Partition] =
    Array(
      MyPartition(0),
      MyPartition(1)
    )
}

/** Партиция - некая метаинформация необходимая для чтения данных. */
case class MyPartition(index: Int) extends Partition