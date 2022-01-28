package l_10

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/**
 * Проверка работы стримового источника в спарк консоли.
 * 1. sbt package =>
 * /home/mike/_learn/repos/newprolab/spark_1/lectures/target/scala-2.13/lectures_2.13-0.1.jar
 *
 * 2.
 * ./spark-shell --jars /home/mike/_learn/repos/newprolab/spark_1/lectures/target/scala-2.13/lectures_2.13-0.1.jar
 *
 * 3.
 * val schemaDDL: String = "`id` INT,`value` STRING"
 * import org.apache.spark.sql.types.StructType
 * val df = spark.readStream.schema(StructType.fromDDL(schemaDDL)).format("org.apache.spark.sql.MyDataSourceProvider").load()
 * df.writeStream.format("console").start()
 */

class ReaderSpec extends AnyFlatSpec with should.Matchers with SparkSupport {

  /** testOnly l_10.ReaderSpec */
  "Reader" should "read" in {

    val schemaDDL: String = "`id` INT,`value` STRING"

    val df: DataFrame = spark
      .readStream
      .schema(StructType.fromDDL(schemaDDL))
      .format("org.apache.spark.sql.MyDataSourceProvider")
      .load()

    val sq: StreamingQuery =
      df
        .writeStream
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .format("console")
        .option("numRows", "50")
        .start()

    sq.awaitTermination(25000)
    sq.stop()

    /**
     * 1. java.lang.ClassNotFoundException: Failed to find data source: org.apache.spark.sql.MyDataSourceProvider
     * => 2. java.lang.UnsupportedOperationException: Data source org.apache.spark.sql.MyDataSourceProvider does not support streamed reading
     * => 3. scala.NotImplementedError: an implementation is missing - sourceSchema call
     * => 4. scala.NotImplementedError: an implementation is missing - createSource call
     * => 5. scala.NotImplementedError: an implementation is missing - getOffset
     * => 6. scala.NotImplementedError: an implementation is missing - getBatch
     * => 7. java.lang.AssertionError: assertion failed: DataFrame returned by getBatch from org.apache.spark.sql.MyDataSource@7bf50ab2 did not have isStreaming=true
     * => 8. java.lang.ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 1 - несоответствие количества элементов в InternalRow и заданной схеме
     * => 9. java.lang.ClassCastException: class java.lang.String cannot be cast to class org.apache.spark.unsafe.types.UTF8String (java.lang.String is in module java.base of loader 'bootstrap'; org.apache.spark.unsafe.types.UTF8String is in unnamed module of loader 'app') - несоответствие типов элементов в InternalRow и заданной схеме
     */
  }
}
