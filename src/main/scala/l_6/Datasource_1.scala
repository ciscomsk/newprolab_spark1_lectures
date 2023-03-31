package l_6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.BufferedSource
import scala.io.Source.fromFile

object Datasource_1 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_6")
    .getOrCreate

  val sc: SparkContext = spark.sparkContext
  import spark.implicits._

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame = spark
    .read
    .options(csvOptions)
    .csv("src/main/resources/l_3/airport-codes.csv")
  
  airportsDf.printSchema()
  airportsDf.show(numRows = 1, truncate = 100, vertical = true)
  println(airportsDf.rdd.getNumPartitions)
  println()

//  airportsDf
//    .write
//    .mode(SaveMode.Overwrite)
//    .csv("src/main/resources/l_6/airports-2.csv")

  /**
   * !!! Т.к. запись распределенная - Spark удалит заголовок из csv файла.
   * !!! При попытке чтения ("header" -> "true") - в качестве схемы Spark возьмет одну из строк, содержащую данные.
   * !!! Схемы необходимо сохранять (например в БД).
   */
  val airportsReadDf: DataFrame = spark
    .read
    .options(csvOptions)
    .csv("src/main/resources/l_6/airports-2.csv")

  airportsReadDf.printSchema
  airportsReadDf.show(numRows = 1, truncate = 100, vertical = true)
  /*
    root
     |-- 00A0: string (nullable = true)
     |-- heliport: string (nullable = true)
     |-- Total Rf Heliport: string (nullable = true)
     |-- 11: integer (nullable = true)
     |-- NA: string (nullable = true)
     |-- US: string (nullable = true)
     |-- US-PA: string (nullable = true)
     |-- Bensalem: string (nullable = true)
     |-- 00A8: string (nullable = true)
     |-- _c9: string (nullable = true)
     |-- 00A10: string (nullable = true)
     |-- 40.07080078125, -74.93360137939453: string (nullable = true)
   */

  val firstLineWritedDf: String = spark
    .read
    .text("src/main/resources/l_6/airports-2.csv")
    .head()
    .toString()

  println(firstLineWritedDf)
  println()

  /** Сохранять схему удобно в json. */
  val airportsDfSchema: StructType = airportsDf.schema
  val airportDFSchemaJson: String = airportsDfSchema.json
  println(airportDFSchemaJson)
  println()

  /** Импорт схемы. */
  val importedSchema: DataType = DataType.fromJson(airportDFSchemaJson)

  /** Чтение со схемой. */
  val csvOptions2: Map[String, String] = Map("header" -> "false", "inferSchema" -> "false")

  val airportsDf2: DataFrame = spark
    .read
    .schema(DataType.fromJson(airportDFSchemaJson).asInstanceOf[StructType])
    .options(csvOptions2)
    .csv("src/main/resources/l_6/airports-2.csv")

  airportsDf2.printSchema()
  airportsReadDf.show(numRows = 1, truncate = 100, vertical = true)
  println(airportsDf2.rdd.getNumPartitions)
  println()

  /** Получение схемы из строки текстового файла. */
  val originalCsv: BufferedSource = fromFile("src/main/resources/l_3/airport-codes.csv")
  val firstLineOriginalCsv: String = originalCsv.getLines.next()
  println(firstLineOriginalCsv)

  val processedSchema: StructType =
    StructType(
      firstLineOriginalCsv
        .split(",", -1)
        .map(el => StructField(el, StringType))
    )

  println(processedSchema)
  println()

  /** Запись с компрессией gzip */
//  airportsDf2
//    .repartition(1)
//    .write
//    .mode(SaveMode.Overwrite)
//    .option("codec", "gzip")
//    .csv("src/main/resources/l_6/airports-3.csv.gz")

  val compressedDf: DataFrame =
    spark
      .read
      .schema(DataType.fromJson(airportDFSchemaJson).asInstanceOf[StructType])
      .options(csvOptions2)
      .csv("src/main/resources/l_6/airports-3.csv.gz")

  /** !!! Сжатый файл при чтении превращается ровно в 1 партицию. Антипаттерн. */
  println(compressedDf.rdd.getNumPartitions)
  println()

  /** Запись с партицированием - долго записывается т.к. много файлов ~ 42k. */
//  airportsDf
//    .repartition(2)
//    .write
//    .mode(SaveMode.Overwrite)
//    .partitionBy("iso_region", "iso_country")
//    .json("src/main/resources/l_6/airports-4.json_partitioned")

  /**
   * !!! Отключение записи _SUCCESS файлов.
   * sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false").
   *
   * Получение файловой системы с которой работает Spark.
   * sc.hadoopConfiguration.get("fs.defaultFS") => file:///... | hdfs://cluster-name/...
   *
   * ФС для записи можно указывать вручную => file:///... | hdfs://cluster-name/...
   * !!! В случае HDFS - имя должно резолвится в нейм-ноду.
   *
   * В случае запуска на ярне - запись на локальную файловую систему работать не будет (каждый воркер будет считать
   * локальной свою файловую систему).
   */

  sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

//  airportsDf
//    .write
//    .mode(SaveMode.Overwrite)
//    .csv("src/main/resources/l_6/airports-5.noSUCCESS")

  println(sc.hadoopConfiguration.get("fs.defaultFS"))

  /**
   * Сохранение датасета в text
   * !!! Для этого датафрейм должен содержать 1 StringType колонку
   */
//  airportsDf
    // err - Text data source supports only a single column, and you have 12 columns
//    .withColumn("elevation_ft", 'elevation_ft.cast(StringType))
    // err - Text data source does not support int data type
//    .write
//    .mode(SaveMode.Overwrite)
//    .format("text")
//    .save("src/main/resources/l_6/airports-6.text")

//  airportsDf
//    .select('ident.as("value"))
//    .write
//    .mode(SaveMode.Overwrite)
//    .format("text")
//    .save("src/main/resources/l_6/airports-6.text")

  /**
   * Файловые форматы не имеют автоматической валидации данных при записи, поэтому достаточно легко
   * ошибиться и записать данные в другом формате.
   *
   * По ошибке запишем в ту же папку данные в формате json - SaveMode.Append:
   */
//  airportsDf
//    .write
//    .mode(SaveMode.Append)
//    .json("src/main/resources/l_6/airports-6.text")

  /** При попытке чтения данных как text мы получим все данные, т.к. формат json сохраняет все в виде JSON строк. */
  val mixedTextDf: DataFrame =
    spark
      .read
      .text("src/main/resources/l_6/airports-6.text")

  mixedTextDf.show(3, truncate = false)

  /** Если прочитать данные с помощью json, часть данных будет помечена как невалидная и помещена в колонку _corrupt_record. */
  val mixedJsonDf: DataFrame =
    spark
      .read
      .json("src/main/resources/l_6/airports-6.text")
    /** Можно читать только файлы определенного формата. */
//      .json("src/main/resources/l_6/airports-6.text/*.json") // | *.txt

  mixedJsonDf.printSchema()
  mixedJsonDf.show(3)

  /** Отобразим невалидные JSON строки. */
  mixedJsonDf
    .na.drop("all", Seq("_corrupt_record"))
    /** Начиная со Spark 2.3 нельзя выбирать одну колонку _corrupt_record. */
    /*
      Exception in thread "main" org.apache.spark.sql.AnalysisException:
      Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
      referenced columns only include the internal corrupt record column
      (named _corrupt_record by default). For example:
      spark.read.schema(schema).csv(file).filter($"_corrupt_record".isNotNull).count()
      and spark.read.schema(schema).csv(file).select("_corrupt_record").show().
      Instead, you can cache or save the parsed results and then send the same query.
      For example, val df = spark.read.schema(schema).csv(file).cache() and then
      df.filter($"_corrupt_record".isNotNull).count().
     */
//    .select($"_corrupt_record")
    .select($"_corrupt_record", $"ident")
    .show(3, truncate = false)


  /**
   * !!! Динамическая перезапись партиций.
   * Без этой опции будут удаляться все партиции, а не только присутствующие в записываемом датасете.
   */
  // v1
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

//  airportsDf
//    .filter('iso_country.isin("RU", "US"))  // 1-й запуск
//    .filter('iso_country.isin("GB", "CN"))  // 2-й запуск
//    .write
    // v2
////    .option("spark.sql.sources.partitionOverwriteMode", "dynamic")
//    .format("json")
//    .mode(SaveMode.Overwrite)
//    .partitionBy("iso_country")
//    .save("src/main/resources/l_6/airports-7.dynamicOverwrite")

  /** Семплирование - чтение определенной части данных для вывода схемы с типами. */
  spark.time {
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true", "samplingRatio" -> "0.1")

    val airportsDf: DataFrame = spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

    airportsDf.printSchema()
  }  // 176 ms

  spark.time {
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true", "samplingRatio" -> "1.0")

    val airportsDf: DataFrame = spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

    airportsDf.printSchema
  }  // 213 ms
}
