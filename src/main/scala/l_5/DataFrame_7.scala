package l_5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, from_json, lit, schema_of_json}
import org.apache.spark.sql.types.{ArrayType, AtomicType, BooleanType, DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object DataFrame_7 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_5")
    .getOrCreate

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._
  println(sc.uiWebUrl)

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame = spark
    .read
    .options(csvOptions)
    .csv("src/main/resources/l_3/airport-codes.csv")

  /** columns - получение списка колонок. */
  println(airportsDf.columns.toList)
  println

  /**
   * schema - получение схемы DF.
   * !!! Поля в схеме всегда упорядочены. */
  val schema: StructType = airportsDf.schema
  println(schema)
  println

  /** apply - получение поля структуры по имени/индексу. */
  val field: StructField = schema("ident") // == schema(0)
  println(field)
  println

  /** fieldIndex - получение индекса поля по имени */
  val idx: Int = schema.fieldIndex("ident")
  println(idx)
  println

  val name: String = field.name
  println(name)

  val fieldType: DataType = field.dataType
  println(fieldType)

  fieldType match {
    case _: StringType => println("This is StringType")
    case _ => println("This is not StringType!")
  }
  println()

  /** simpleString - получение DDL схемы в виде удобочитаемой строки. */
  println(fieldType.simpleString)
  println(schema.simpleString)
  println()

  /** schema.json/DataType.fromJson(schema) - удобно при необходимости сериализации и передачи схемы. */
  val jsonSchema: String = schema.json
  println(jsonSchema)
  println(DataType.fromJson(jsonSchema))
  println()

  val ddlSchema: String = schema.toDDL
  println(ddlSchema)
  println(DataType.fromDDL(ddlSchema))
  println

  /** Создание схемы из кейс класса. */
  case class Airport(
                      ident: String,
                      `type`: String,
                      name: String,
                      elevation_ft: Int,
                      continent: String,
                      iso_country: String,
                      iso_region: String,
                      municipality: String,
                      gps_code: String,
                      iata_code: String,
                      local_code: String,
                      coordinates: String
                    )

  val schemaFromCC: StructType =
    ScalaReflection
      .schemaFor[Airport]
      .dataType
      .asInstanceOf[StructType]

  println(schemaFromCC)

  /** Получение схемы через ds. */
  val ds: Dataset[Airport] = spark.emptyDataset[Airport]
  println(ds.schema)
  println()

  /** Использование схемы. */
  val airportsSchemaDf: DataFrame = spark
    .read
    .options(csvOptions)
    .schema(schemaFromCC)
    .csv("src/main/resources/l_3/airport-codes.csv")

  airportsSchemaDf.printSchema()
  airportsSchemaDf.show(numRows = 1, truncate = 100, vertical = true)

  val parseJson: Column = from_json(col("value"), schemaFromCC).alias("s")

  val jsonedDf: Dataset[String] = airportsDf.toJSON // в датафрейме будет 1 колонка value
  jsonedDf.printSchema()

  val withColumnsDf: DataFrame = jsonedDf
    .select(parseJson)
    .select(col("s.*"))

  withColumnsDf.show(1, 200, vertical = true)
  withColumnsDf.printSchema()

  /** Схема может быть создана вручную. */
  val someSchema: StructType =
    StructType(List(
      StructField("foo", StringType),
      StructField("bar", StringType),
      StructField("boo",
        StructType(List(
          StructField("x", IntegerType),
          StructField("y", BooleanType)
        ))
      )
    ))

  someSchema.printTreeString

  /** Схема может быть получена из JSON строки. */
  val firstLine: String = jsonedDf.head
  println(firstLine)

  val row: Row = spark
    .range(1)
    .select(schema_of_json(lit(firstLine)))
    .head

  println(row)

  /** cast - изменяет тип колонки, может возвращать null - при некорректном касте. */
  airportsDf
    .select('elevation_ft.cast(StringType)) // StringType можно заменить на string
    .printSchema

  airportsDf
    .select('type.cast("float"))
    .printSchema

  airportsDf
    .select('type.cast("float"))
    .show(1, truncate = false)

  /** !!! cast может менять названия/типы колонок внутри структуры, но не список колонок (например - убрать нельзя, это можно сделать с помощью рекурсии). */
  // cast(StructType(...))

  Thread.sleep(1000000)
}
