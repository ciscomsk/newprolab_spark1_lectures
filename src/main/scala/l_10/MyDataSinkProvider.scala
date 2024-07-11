package l_10

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

class MyDataSinkProvider extends StreamSinkProvider with Logging {
  /** Для работы логирования нужно добавить настройки для пакета или класса в log4j2.properties */
  log.info(s"${this.logName} has been created")

  override def createSink(
                           sqlContext: SQLContext, // ~SparkSession
                           parameters: Map[String, String], // то, что было передано в writeStream.options
                           partitionColumns: Seq[String], // то, что было передано в writeStream.partitionBy
                           outputMode: OutputMode // то, что было передано в writeStream.outputMode
                         ): Sink = {

    log.info(s"parameters: ${parameters.mkString(" ")}")
    log.info(s"partitionColumns: ${partitionColumns.mkString(" ")}")
    log.info(s"outputMode: $outputMode")

    new MyDataSink
  }
}

class MyDataSink extends Sink with Logging {
  log.info(s"${this.logName} has been created")

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    log.info(s"batchId: $batchId, data: $data")

    /**
     * ERROR MicroBatchExecution: Query [id = f7a20aaa-97d9-4324-b738-359f166b12d5, runId = 4dbd164b-8a05-4aad-b4bd-777471c6df52] terminated with error
     * org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start()
     *
     * !!! Ошибки при вызове show/getNumPartitions - говорят о том, что датафрейм стримовый
     */
//    data.show()
//    log.info(s"data.rdd.getNumPartitions: ${data.rdd.getNumPartitions}")
    log.info(s"df.isStreaming: ${data.isStreaming}") // true

    val schema: StructType = data.schema
//    println(schema) // StructType(StructField(timestamp,TimestampType,true),StructField(value,LongType,true))
    val rdd: RDD[InternalRow] = data.queryExecution.toRdd

    rdd.foreachPartition { partition =>
      /** Итератор - это ленивая коллекция */
      val thisPartition: Iterator[InternalRow] = partition

      /**
       * InternalRow - абстрактный класс, описывающий то, как хранятся данные в датафрейме "под капотом"
       * В отличие от Row, в InternalRow не инкапсулирована схема (нет схемы внутри)
       *
       * Имплементации InternalRow:
       * 1. Generic Internal Row - on heap - массив объектов - Array[Any]
       * 2. Unsafe Row - off heap - java.nio.bytebuffer
       */

      /** Для v3 */
      val fieldNames: String = schema.map(_.name).mkString(", ")
//
      while(partition.hasNext) {
        val nextItem: InternalRow = thisPartition.next()

        /** Получаем данные из InternalRow - схема берется из датафрейма */
        val columns: Seq[Any] = nextItem.toSeq(schema)
//        println(columns) // ArraySeq(1716453864131000, 2)

        /**
         * Далее пишется логика преобразования данных к формату, который потребляет синк
         * например - INSERT INTO для БД/преобразование в Arrow/Protobuf/...
         */

        /** v1 - просто печать */
//        columns
//          .zip(schema)
//          .foreach { case (item, field) =>
//            println(s"field: ${field.name}, type: ${field.dataType.simpleString}, value: $item")
//          }
        /*
          field: timestamp, type: timestamp, value: 1716453864131000
          field: value, type: bigint, value: 2
         */

        /** v2 - заготовка для записи в синк с матчингом по типу колонки */
//        columns
//          .zip(schema)
//          .foreach { case (item, field) =>
//            (item, field.dataType) match {
//              case (value: java.lang.Long, LongType) =>
//                println(s"name: ${field.name}, type: ${field.dataType.simpleString}, value: $value")
//
//              case (value: java.lang.Long, TimestampType) =>
//                println(s"name: ${field.name}, type: ${field.dataType.simpleString}, value: $value")
//
//              case (value: UTF8String, StringType) =>
//                println(s"name: ${field.name}, type: ${field.dataType.simpleString}, value: $value")
//
//              case (value, fieldType) =>
//                throw new UnsupportedOperationException(s"$value of type ${fieldType.simpleString} is not supported")
//            }
//          }
        /*
          name: timestamp, type: timestamp, value: 1716453864131000
          name: value, type: bigint, value: 2
         */

        /** v3 - пример с генерацией запроса для записи в БД */
        val fieldValues: String =
          columns
            .zip(schema.map(_.dataType))
            .map {
              case (value: java.lang.Long, LongType) =>
                s"${value.toString}L"

              case (value: java.lang.Long, TimestampType) =>
                s"ts:${value.toString}"

              case (value: UTF8String, StringType) =>
                s"""'${value.toString}'"""

              /**
               * Для сложных типов:
               * 1. struct -> value: InternalRow
               * 2. array -> value: ArrayData
               */
            }
            .mkString(", ")

        val insertQuery: String = s"INSERT INTO TABLE foo ($fieldNames) VALUE ($fieldValues)"
        println(insertQuery)
        // INSERT INTO TABLE foo (timestamp, value) VALUE (ts:1716453864131000, 2L)
      }
    }
  }
}
