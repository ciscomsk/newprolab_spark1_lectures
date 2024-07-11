package l_5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.lang

class InternalRowSpec extends AnyFlatSpec with should.Matchers {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  val df: Dataset[lang.Long] = spark.range(0, 100)
  val schema: StructType = df.schema
  println(schema)

  val iRdd: RDD[InternalRow] =
    df
      .queryExecution
      .toRdd

  println(iRdd)

  val iRow: InternalRow =
    iRdd
      .collect()
      .head

  println(s"iRow: $iRow")

  val data: Seq[Any] = iRow.toSeq(schema)
  println(s"data: $data")

//  data.foreach {
//  }
}
