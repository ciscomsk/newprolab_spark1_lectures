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

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_6")
      .getOrCreate

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  import spark.implicits._

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

  println()
  airportsDf.printSchema()
  airportsDf.show(numRows = 1, truncate = 100, vertical = true)
  println(airportsDf.rdd.getNumPartitions)
  println()

//  airportsDf
//    .write
//    .mode(SaveMode.Overwrite)
//    .csv("src/main/resources/l_6/airports-2.csv")
  /**
   * !!! т.к. запись распределенная - Spark удаляет заголовок из csv файла
   * при попытке чтения ("header" -> "true") таких данных - в качестве схемы Spark возьмет одну из строк, содержащую данные
   *
   * схему нужно сохранять (например в БД)
   */

  val airportsReadDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_6/airports-2.csv")

  airportsReadDf.printSchema()
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

  val headerLinesCount: Long =
    spark
      .read
      .text("src/main/resources/l_6/airports-2.csv")
      .filter($"value".contains("elevation_ft"))
      .count()

  println(s"headerLinesCount: $headerLinesCount")
  println()

  val firstLineWritedDf: String =
    spark
      .read
      .text("src/main/resources/l_6/airports-2.csv")
      .head()
      .toString()

  println(firstLineWritedDf)
  println()

  /** если прочитать с header == false - названия колонок будут сгенерированы автоматически - _с0/_с1/... */
  val csvOptions2: Map[String, String] = Map("header" -> "false", "inferSchema" -> "true")

  spark
    .read
    .options(csvOptions2)
    .csv("src/main/resources/l_6/airports-2.csv")
    .printSchema()

  /** сохранять схему удобно в json */
  val airportDfSchemaJson: String = airportsDf.schema.json
  println(airportDfSchemaJson)
  println()

  /** импорт схемы */
  val importedSchema: DataType = DataType.fromJson(airportDfSchemaJson)
  println(importedSchema)
  println()

  /** чтение со схемой */
  val csvOptions3: Map[String, String] = Map("header" -> "false", "inferSchema" -> "false")

  val airportsDf2: DataFrame =
    spark
      .read
      .schema(DataType.fromJson(airportDfSchemaJson).asInstanceOf[StructType])
      .options(csvOptions3)
      .csv("src/main/resources/l_6/airports-2.csv")

  airportsDf2.printSchema()
  airportsDf2.show(numRows = 1, truncate = 100, vertical = true)
  println(s"airportsDf2.rdd.getNumPartitions: ${airportsDf2.rdd.getNumPartitions}")
  println()

  /** получение схемы из шапки csv */

  /** v1 - с помощью команд терминала */
  import sys.process._
  val originalCsvTerminal: String = "head -n 1 src/main/resources/l_3/airport-codes.csv".!!
  println(originalCsvTerminal)

  /** v2 - c помощью scala.io */
  val originalCsvIO: BufferedSource = fromFile("src/main/resources/l_3/airport-codes.csv")
  val firstLineOriginalCsv: String = originalCsvIO.getLines.next()
  println(firstLineOriginalCsv)

  val processedSchema: StructType =
    StructType(
      firstLineOriginalCsv
        .split(",", -1)
        .map(el => StructField(el, StringType))
    )

  println(processedSchema)
  println()

  /** запись с компрессией gzip */
//  airportsDf2
//    .repartition(1)
//    .write
//    .mode(SaveMode.Overwrite)
//    .option("codec", "gzip")
//    .csv("src/main/resources/l_6/airports-3.csv.gz")

  val compressedDf: DataFrame =
    spark
      .read
      .schema(DataType.fromJson(airportDfSchemaJson).asInstanceOf[StructType])
      .options(csvOptions2)
      .csv("src/main/resources/l_6/airports-3.csv.gz")

  /** !!! сжатый файл при чтении превращается ровно в 1 партицию - антипаттерн */
  println(s"compressedDf.rdd.getNumPartitions: ${compressedDf.rdd.getNumPartitions}")
  println()

  /** запись с партицированием - длительный процесс т.к. файлов ~ 42k */
//  airportsDf
//    .repartition(2)
//    .write
//    .mode(SaveMode.Overwrite)
//    .partitionBy("iso_region", "iso_country")
//    .json("src/main/resources/l_6/airports-4.json_partitioned")

  val lsRes: Int =
    "ls -laR src/main/resources/l_6/airports-4.json_partitioned".!!
      .split("\n")
      .length

  println(lsRes)
  println()

  /**
   * !!! отключение записи _SUCCESS файлов
   * sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
   *
   * получение файловой системы с которой работает Spark
   * sc.hadoopConfiguration.get("fs.defaultFS") => file:///... | hdfs://name-node/...
   *
   * ФС для записи можно указывать вручную => file:///... | hdfs://name-node/...
   * в случае HDFS - cluster-name должно резолвится в нейм-ноду
   *
   * в случае работы на ярне - запись на локальную файловую систему работать не будет (каждый воркер будет считать
   * локальной свою файловую систему)
   */

  println(sc.hadoopConfiguration.get("fs.defaultFS"))
  println()

  sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

//  airportsDf
//    .write
//    .mode(SaveMode.Overwrite)
//    .csv("src/main/resources/l_6/airports-5.noSUCCESS")

  /** сохранение датасета в text - датафрейм должен содержать 1 StringType колонку */

  // err - Column `elevation_ft` has a data type of int, which is not supported by Text.
//  airportsDf
//    .write
//    .mode(SaveMode.Overwrite)
//    .format("text")
//    .save("src/main/resources/l_6/airports-6.text")

  // err - Text data source supports only a single column, and you have 12 columns
//  airportsDf
//    .withColumn("elevation_ft", $"elevation_ft".cast(StringType))
//    .write
//    .mode(SaveMode.Overwrite)
//    .format("text")
//    .save("src/main/resources/l_6/airports-6.text")

  // ок
//  airportsDf
//    .select($"ident".as("value"))
//    .write
//    .mode(SaveMode.Overwrite)
//    .format("text")
//    .save("src/main/resources/l_6/airports-6.text")

  /**
   * !!! файловые форматы не имеют автоматической валидации данных при записи
   * => достаточно легко ошибиться и записать данные в другом формате
   */

  /** как бы по ошибке запишем в ту же папку данные в формате json - SaveMode.Append */
//  airportsDf
//    .write
//    .mode(SaveMode.Append)
//    .json("src/main/resources/l_6/airports-6.text")

  /** при попытке чтения данных как text получим все данные, т.к. формат json сохраняет все в виде JSON строк */
  val mixedTextDf: DataFrame =
    spark
      .read
      .text("src/main/resources/l_6/airports-6.text")

  println("mixedTextDf: ")
  mixedTextDf.show(3, truncate = false)

  /** если читать данные как json, часть данных будет помечена как невалидная и помещена в колонку _corrupt_record */
  val mixedJsonDf: DataFrame =
    spark
      .read
      .json("src/main/resources/l_6/airports-6.text")
    /** можно читать только файлы определенного формата - *.'format' */
//      .json("src/main/resources/l_6/airports-6.text/*.json")
//      .json("src/main/resources/l_6/airports-6.text/*.txt")

  println("mixedJsonDf: ")
  mixedJsonDf.printSchema()
  mixedJsonDf.show(3)

  /** отобразим невалидные JSON строки */
  println("invalid data: ")
  mixedJsonDf
    .na.drop("all", Seq("_corrupt_record")) // у валидных строк _corrupt_record == null
    /** начиная со Spark 2.3 нельзя выбирать только колонку _corrupt_record */
    /*
      Exception in thread "main" org.apache.spark.sql.AnalysisException:
      Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
      referenced columns only include the internal corrupt record column
      (named _corrupt_record by default).
      For example:
      spark.read.schema(schema).csv(file).filter($"_corrupt_record".isNotNull).count()
      and spark.read.schema(schema).csv(file).select("_corrupt_record").show().
      Instead, you can cache or save the parsed results and then send the same query.
      For example,
      val df = spark.read.schema(schema).csv(file).cache() and then
      df.filter($"_corrupt_record".isNotNull).count()
     */
//    .select($"_corrupt_record")
    .select($"_corrupt_record", $"ident")
    .show(3, truncate = false)


  /**
   * !!! динамическая перезапись партиций
   * без этой опции будут удаляться все партиции, а не только присутствующие в записываемом датасете
   */
  // v1
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

//  airportsDf
//    .filter($"iso_country".isin("RU", "US")) // 1-й запуск
//    .filter($"iso_country".isin("GB", "CN")) // 2-й запуск
//    .write
    // v2
//    .option("spark.sql.sources.partitionOverwriteMode", "dynamic")
//    .format("json")
//    .mode(SaveMode.Overwrite)
//    .partitionBy("iso_country")
//    .save("src/main/resources/l_6/airports-7.dynamicOverwrite")

  /** семплирование - чтение определенной части данных для вывода схемы с типами */
  spark.time {
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true", "samplingRatio" -> "0.1")

    val airportsDf: DataFrame =
      spark
        .read
        .options(csvOptions)
        .csv("src/main/resources/l_3/airport-codes.csv")

    airportsDf.printSchema()
  } // 160 ms
  println()

  spark.time {
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true", "samplingRatio" -> "1.0")

    val airportsDf: DataFrame =
      spark
        .read
        .options(csvOptions)
        .csv("src/main/resources/l_3/airport-codes.csv")

    airportsDf.printSchema
  } // 201 ms
  println()


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
