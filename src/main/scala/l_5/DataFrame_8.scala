package l_5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, from_json, lit, schema_of_json}
import org.apache.spark.sql.types.{ArrayType, AtomicType, BooleanType, DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

/** test: l_5 + org.apache.spark.sql  */
object DataFrame_8 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  import spark.implicits._

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

  /** columns - получение списка колонок */
  println()
  println(airportsDf.columns.toList)
  println()

  /**
   * schema - получение схемы DF
   * !!! порядок полей в схеме (в StructType) ВАЖЕН
   */
  val schema: StructType = airportsDf.schema
  println(schema)
  println()

  /** apply - получение поля по имени/индексу */
  val field: StructField = schema("ident") // == schema(0)
  println(field)
  println()

  /** fieldIndex - получение индекса поля по имени */
  val idx: Int = schema.fieldIndex("ident")
  println(idx)
  println()

  val fieldName: String = field.name
  val fieldType: DataType = field.dataType
  println(fieldName)
  println(fieldType)

  fieldType match {
    case _: StringType => println("This is StringType")
    case _ => println("This is not StringType!")
  }
  println()

  /** simpleString - получение DDL схемы в виде удобочитаемой строк */
  println("simpleString: ")
  println(fieldType.simpleString)
  println(schema.simpleString)
  println()

  /** schema.json/DataType.fromJson(schema) - удобно при необходимости сериализации и передачи схемы */
  val jsonSchema: String = schema.json
  val schemaFromJson: DataType = DataType.fromJson(jsonSchema)
  println("json: ")
  println(jsonSchema)
  println(schemaFromJson)
  println()

  println("DataType: ")
  val ddlSchema: String = schema.toDDL
  val schemaFromDtDDL: DataType = DataType.fromDDL(ddlSchema)
  println(ddlSchema)
  println(schemaFromDtDDL)
  println()


  println("schema.sql: ")
  val sqlSchema: String = schema.sql
  println(sqlSchema)
  println()

  println("StructType: ")
//  val schemaFromStDDL: StructType = StructType.fromDDL(sqlSchema) // org.apache.spark.sql.catalyst.parser.ParseException
  val schemaFromStDDL: StructType = StructType.fromDDL(ddlSchema) // ок
  println(schemaFromStDDL)
  println()

  /**
   * Получение схемы из кейс класса
   * v1.1 - reflection
   */
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

  /** v1.2 - DS API */
  val ds: Dataset[Airport] = spark.emptyDataset[Airport]
  println(ds.schema)
  println()

  /** использование схемы */
  val airportsSchemaDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .schema(schemaFromCC)
      .csv("src/main/resources/l_3/airport-codes.csv")

  airportsSchemaDf.printSchema()
  airportsSchemaDf.show(numRows = 1, truncate = 100, vertical = true)

  val jsonedDf: Dataset[String] = airportsDf.toJSON // в датафрейме будет 1 колонка value
  jsonedDf.show(1, truncate = false)
  jsonedDf.printSchema()

  val parseJson: Column = from_json(col("value"), schemaFromCC).alias("s")

  val withColumnsDf: DataFrame =
    jsonedDf
      .select(parseJson)
      .select(col("s.*"))

  withColumnsDf.show(1, 200, vertical = true)
  withColumnsDf.printSchema()

  /** v2 - ручное создание схемы */
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

  someSchema.printTreeString()
  println()

  /** v3 - получение схемы из json */
  val firstLine: String = jsonedDf.head()
  println(s"firstLine: $firstLine")

  val row: Row =
    spark
      .range(1)
      .select(schema_of_json(lit(firstLine)))
      .head()

  println(s"row: $row")
  println()

  /** cast - изменяет тип колонки, возвращает null при некорректном касте */
  airportsDf
    .select($"elevation_ft".cast(StringType)) // StringType == string
    .printSchema()

  airportsDf
    .select($"type".cast("float"))
    .printSchema()

  airportsDf
    .select($"type".cast("float"))
    .show(1, truncate = false)

  /**
   * !!! cast может менять названия/типы колонок внутри структуры
   * но не список этих колонок (например - убрать нельзя, это можно сделать с помощью рекурсии)
   */
  // cast(StructType(...))


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
