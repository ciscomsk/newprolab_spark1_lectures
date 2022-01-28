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
  /** Для работы логирования нужно добавить класс/package в log4j.properties */
  log.info(s"${this.logName} has been created")

  override def createSink(sqlContext: SQLContext, // ~SparkSession
                          parameters: Map[String, String],  // то, что будет передано в writeStream.options
                          partitionColumns: Seq[String],  // то, что будет передано в writeStream.partitionBy
                          outputMode: OutputMode): Sink = {

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
    log.info(s"df.isStreaming: ${data.isStreaming}")

    /** err - org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start(); */
//    data.show() // exception т.к. датафрейм здесь стримовый
//    log.info(s"${data.rdd.getNumPartitions}") // exception

    val schema: StructType = data.schema
    println(schema)  // StructType(StructField(timestamp,TimestampType,true), StructField(value,LongType,true))

    val fieldNames: String = schema.map(_.name).mkString(", ")

    val rdd: RDD[InternalRow] = data.queryExecution.toRdd

    rdd.foreachPartition { partition =>

      /** Итератор - ленивая коллекция. */
      /**
       * InternalRow - абстрактный класс, описывающий то, как хранятся данные в датафрейме под капотом.
       * В отличие от Row, в InternalRow не инкапсулирована схема (нет схемы внутри).
       *
       * Имплементации InternalRow:
       * 1. Generic Internal Row - on heap (массив объектов - Array[Any]).
       * 2. Unsafe Row - off heap (java.nio.bytebuffer).
       */
      val thisPartition: Iterator[InternalRow] = partition

      while (thisPartition.hasNext) {
        val nextItem: InternalRow = thisPartition.next()
        /** Получаем данные из InternalRow. Схема берется из датафрейма. */
        val columns: Seq[Any] = nextItem.toSeq(schema)
//        println(columns)  // WrappedArray(timestamp TimestampType == 1643202075307000, value LongType == 6)

        /**
         * Здесь будет логика преобразования данных к формату, который потребляет синк.
         * Что-то вроде INSERT INTO/Arrow/Protobuff/...
         */
//        columns
//          .zip(schema)
//          .foreach { case (item, field) =>
////            println(s"fieldName: ${field.name}, fieldType: ${field.dataType.simpleString}, value: $item")
//
//            (item, field.dataType) match {
//              case (v: java.lang.Long, LongType) =>
//                println(s"fieldName: ${field.name}, fieldType: ${field.dataType.simpleString}, value: $v")
//
//              case (v: java.lang.Long, TimestampType) =>
//                println(s"fieldName: ${field.name}, fieldType: ${field.dataType.simpleString}, value: $v")
//
//              case (v: UTF8String, StringType) =>
//                println(s"fieldName: ${field.name}, fieldType: ${field.dataType.simpleString}, value: $v")
//
//              case (value, fieldType) =>
//                throw new UnsupportedOperationException(s"$value: of type: ${fieldType.simpleString} is not supported!")
//            }
//          }

        /** Пример с генерацией запроса для записи в БД: */
        val fieldValues: String =
          columns
            .zip(schema.map(_.dataType))
            .map {
              case (item: java.lang.Long, LongType) =>
                s"${item.toString}L"

              case (item: java.lang.Long, TimestampType) =>
                s"ts:${item.toString}"  // для простоты

              case (item: UTF8String, StringType) =>
                s""" '${item.toString}' """

              /**
               * В случае сложных типов:
               * 1. struct => item: InternalRow
               * 2. array => item: ArrayData
               */
            }
            .mkString(", ")

        val insertQuery: String = s"INSERT INTO TABLE foo ($fieldNames) VALUE ($fieldValues)"
        println(insertQuery)
      }
    }
  }
}
