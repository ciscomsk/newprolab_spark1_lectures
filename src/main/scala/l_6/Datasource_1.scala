package l_6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Try, Using}

object Datasource_1 extends App {
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
  airportsReadDf.show(numRows = 1, truncate = 100, vertical = true)

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

  println(s"firstLineWritedDf: $firstLineWritedDf")
  println()

  /** если прочитать с header == false - названия колонок будут сгенерированы автоматически - _с0/_с1/... */
  val csvOptions2: Map[String, String] = Map("header" -> "false", "inferSchema" -> "true")

  spark
    .read
    .options(csvOptions2)
    .csv("src/main/resources/l_6/airports-2.csv")
    .printSchema()

  /** сохранение схемы в json */
  val airportDfSchemaJson: String = airportsDf.schema.json
  println(s"airportDfSchemaJson: $airportDfSchemaJson")
  println()

  /** импорт схемы */
  val importedSchema: DataType = DataType.fromJson(airportDfSchemaJson)
  println(s"importedSchema: $importedSchema")
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

  /** Получение схемы из шапки csv */
  /** v1.1 - с помощью команд терминала */
  import sys.process._
  val firstLineTerminal: String = "head -n 1 src/main/resources/l_3/airport-codes.csv".!!
  println(firstLineTerminal)

  /** v1.2 - c помощью scala.io */
  val firstLineIO: String =
    Using.resource(scala.io.Source.fromFile("src/main/resources/l_3/airport-codes.csv")) { bs =>
    bs.getLines().next()
  }
  println(firstLineIO)
  println()

  val processedSchema: StructType =
    StructType(
      firstLineTerminal
        .split(",", -1)
        .map(el => StructField(el, StringType))
    )

  println(s"processedSchema: $processedSchema")
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

  /** !!! сжатый файл читается в 1 партицию - антипаттерн */
  println(s"compressedDf.rdd.getNumPartitions: ${compressedDf.rdd.getNumPartitions}")
  println()

  /** запись с партицированием - длительный процесс т.к. файлов ~ 43k */
//  airportsDf
//    .repartition(2)
//    .write
//    .mode(SaveMode.Overwrite)
//    .partitionBy("iso_region", "iso_country")
//    .json("src/main/resources/l_6/airports-4.json_partitioned")

  val lsCount: Int =
    "ls -laR src/main/resources/l_6/airports-4.json_partitioned".!!
      .split("\n")
      .length

  println(lsCount)
  println()

  val ls: String = "ls -lah src/main/resources/l_6/airports-4.json_partitioned/iso_region=AD-04/iso_country=AD".!!
  println(ls)

  /** !!! колонки партицирования не входят в состав записанных файлов */
  val cat: String =
    "cat src/main/resources/l_6/airports-4.json_partitioned/iso_region=AD-04/iso_country=AD/part-00000-6b21a22e-2f06-4813-8f75-59b4cb7a41a8.c000.json".!!

  println(cat)


  /**
   * !!! Отключение записи _SUCCESS файлов
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
//    .csv("src/main/resources/l_6/airports-5.no_SUCCESS")

  /** сохранение датасета в text - датафрейм должен содержать 1 StringType колонку */
  // org.apache.spark.sql.AnalysisException : [UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE] The Text datasource doesn't support the column `elevation_ft` of the type "INT"
//  airportsDf
//    .write
//    .mode(SaveMode.Overwrite)
//    .format("text")
//    .save("src/main/resources/l_6/airports-6.text")

  // org.apache.spark.sql.AnalysisException : Text data source supports only a single column, and you have 12 columns
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
////    .save("src/main/resources/l_6/airports-6.text")
//    .save("src/main/resources/l_6/airports-7.multiformat")

  /**
   * !!! файловые форматы не имеют автоматической валидации данных при записи
   * => достаточно легко ошибиться и записать данные в другом формате
   */

  /** "по ошибке" запишем данные в формате json в папку с text - SaveMode.Append */
//  airportsDf
//    .write
//    .mode(SaveMode.Append)
//    .json("src/main/resources/l_6/airports-7.multiformat")

  /** при попытке чтения данных как text получим все данные, т.к. формат json сохраняет все в виде JSON строк */
  val mixedTextDf: DataFrame =
    spark
      .read
      .text("src/main/resources/l_6/airports-7.multiformat")

  println("mixedTextDf: ")
  mixedTextDf.show(3, truncate = false)

  /** если читать данные как json, часть данных будет помечена как невалидная и помещена в колонку _corrupt_record */
  val mixedJsonDf: DataFrame =
    spark
      .read
      .json("src/main/resources/l_6/airports-7.multiformat")
    /** можно читать только файлы определенного формата - *.<format> */
//      .json("src/main/resources/l_6/airports-7.multiformat/*.json")
//      .json("src/main/resources/l_6/airports-7.multiformat/*.txt")

  println("mixedJsonDf: ")
  mixedJsonDf.printSchema()
  mixedJsonDf.show(3)

  /** отобразим невалидные JSON строки */
  println("invalid data: ")

  /** начиная со Spark 2.3 нельзя выбирать только колонку _corrupt_record */
  /*
    Exception in thread "main" org.apache.spark.sql.AnalysisException: Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
    referenced columns only include the internal corrupt record column
    (named _corrupt_record by default). For example:
    spark.read.schema(schema).csv(file).filter($"_corrupt_record".isNotNull).count()
    and spark.read.schema(schema).csv(file).select("_corrupt_record").show().
    Instead, you can cache or save the parsed results and then send the same query.
    For example, val df = spark.read.schema(schema).csv(file).cache() and then
    df.filter($"_corrupt_record".isNotNull).count()
   */
//  mixedJsonDf
//    .na.drop("all", Seq("_corrupt_record"))
//    .select($"_corrupt_record")
//    .show(3, truncate = false)

  mixedJsonDf
    .na.drop("all", Seq("_corrupt_record")) // у валидных строк _corrupt_record == null
    .select($"_corrupt_record", $"ident")
    .show(3, truncate = false)


  /**
   * !!! Динамическая перезапись партиций
   * без этой опции будут удаляться все партиции, а не только присутствующие в записываемом датасете
   */
  // v1
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

//  airportsDf
//    .filter($"iso_country".isin("RU", "US")) // 1-й запуск
////    .filter($"iso_country".isin("GB", "CN")) // 2-й запуск
//    .write
//    // v2
////    .option("partitionOverwriteMode", "dynamic")
//    .format("json")
//    .mode(SaveMode.Overwrite)
//    .partitionBy("iso_country")
//    .save("src/main/resources/l_6/airports-8.dynamic_overwrite")


  /** Семплирование - чтение определенной части данных для вывода схемы */
  spark.time {
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true", "samplingRatio" -> "0.1")

    val airportsDf: DataFrame =
      spark
        .read
        .options(csvOptions)
        .csv("src/main/resources/l_3/airport-codes.csv")

    airportsDf.printSchema()
  } // 170 ms
  println()

  spark.time {
    val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true", "samplingRatio" -> "1.0")

    val airportsDf: DataFrame =
      spark
        .read
        .options(csvOptions)
        .csv("src/main/resources/l_3/airport-codes.csv")

    airportsDf.printSchema
  } // 205 ms
  println()


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
