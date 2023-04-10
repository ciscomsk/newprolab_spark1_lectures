package org.apache.spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class SchemaSpec extends AnyFlatSpec with should.Matchers {
  // не работает в Spark 3.3.2
//  Logger
//    .getLogger("org")
//    .setLevel(Level.OFF)

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

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
      /** !!! AtomicType/FractionalType/IntegralType - доступны только в org.apache.spark.sql */
      case a: AtomicType =>  // FractionalType/IntegralType
        println(s"This is atomic type of type ${a.simpleString}")
        a

      case s: StructType =>
        println(s"This is struct which contains the following fields: ${s.fields.toList}")

        val res: Array[StructField] =
          s
            .fields
            .map { field =>
              StructField(field.name, recursion(field.dataType))
            }

        StructType(res)

      case arr: ArrayType =>
        println(s"This is array which contains elements of type ${arr.elementType.simpleString}")
        recursion(arr.elementType)
    }
  }

  "Schema recursion" should "work" in {
    val res: DataType = recursion(someSchema)

    println()
    println(res)
  }

  /** sbt - testOnly org.apache.spark.sql.SchemaSpec */

  def toUpperCase(schema: StructType): StructType = {
    val upperCaseFields: Array[StructField] =
      schema
        .fields
        .map {
          case st @ _ if !st.dataType.isInstanceOf[StructType] =>
            StructField(st.name.toUpperCase, st.dataType, st.nullable, st.metadata)
          case st @ _ if st.dataType.isInstanceOf[StructType] =>
            val struct: StructType = st.dataType.asInstanceOf[StructType]
            StructField(st.name.toUpperCase, toUpperCase(struct), st.nullable, st.metadata)
        }

    StructType(upperCaseFields.toSeq)
  }

  "Schema uppercase" should "work" in {
    val res: DataType = toUpperCase(someSchema)

    println(someSchema)
    println(res)
  }
}
