package org.apache.spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class SchemaSpec extends AnyFlatSpec with should.Matchers {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  val someSchema: StructType =
    StructType(
      List(
        StructField("foo", StringType),
        StructField("bar", StringType),
        StructField("boo",
          StructType(
            List(
              StructField("x", IntegerType),
              StructField("y", BooleanType)
            )
          )
        )
      )
    )

  def recursion(schema: DataType): DataType = {
    schema match {
      /** !!! AtomicType/FractionalType/IntegralType - protected[sql] => доступны только в org.apache.spark.sql */
      case atomic: AtomicType =>
        println(s"This is atomic type of type ${atomic.simpleString}")
        atomic

      case struct: StructType =>
        println(s"This is struct which contains the following fields: ${struct.fields.toList}")

        struct
          .fields
          .map(field => StructField(field.name, recursion(field.dataType)))

        struct

      case array: ArrayType =>
        println(s"This is array which contains elements of type ${array.elementType.simpleString}")
        recursion(array.elementType)
    }
  }

  "Schema recursion" should "work" in {
    val result: DataType = recursion(someSchema)
    println()

    println(s"res: $result")
  }

  /** sbt shell => testOnly org.apache.spark.sql.SchemaSpec */
  def toUpperCase(schema: StructType): StructType = {
    val upperCaseFields: Array[StructField] =
      schema
        .fields
        .map {
          case sf if !sf.dataType.isInstanceOf[StructType] =>
            StructField(sf.name.toUpperCase, sf.dataType, sf.nullable, sf.metadata)
          case sf if sf.dataType.isInstanceOf[StructType] =>
            val struct: StructType = sf.dataType.asInstanceOf[StructType]
            StructField(sf.name.toUpperCase, toUpperCase(struct), sf.nullable, sf.metadata)
        }

    StructType(upperCaseFields)
  }

  "Schema uppercase" should "work" in {
    val res: StructType = toUpperCase(someSchema)

    println(someSchema)
    println(res)
  }
}
