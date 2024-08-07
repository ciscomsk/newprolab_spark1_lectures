package l_6

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object Datasource_2 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_6")
      /**
       * java.lang.IllegalArgumentException: Unable to instantiate SparkSession with Hive support because Hive classes are not found
       * -> build.sbt "spark-hive"
       */
      .enableHiveSupport()
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  import spark.implicits._

  val csvOption: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOption)
      .csv("src/main/resources/l_3/airport-codes.csv")

  println()
  airportsDf.printSchema()
  println(airportsDf.count())
  println()

  /** !!! Отключение записи .crc файлов  */
  val hadoopConf: Configuration = sc.hadoopConfiguration
  FileSystem.get(hadoopConf).setWriteChecksum(false)

//  airportsDf
//    .repartition(1)
//    .write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/l_6/airports-9.parquet")

  val parquetDf: DataFrame =
    spark
      .read
      .parquet("src/main/resources/l_6/airports-9.parquet")

  parquetDf.printSchema()
  println(parquetDf.rdd.getNumPartitions)
  println()

  /** в каждой row group для каждой колонки рассчитываются min/max значения -> фильтр будет спущен в PushedFilters */
  parquetDf
    .filter($"iso_country" === "RU")
    .explain()
  /*
    == Physical Plan ==
    *(1) Filter (isnotnull(iso_country#63) AND (iso_country#63 = RU))
    +- *(1) ColumnarToRow
       // PushedFilters: [IsNotNull(iso_country), EqualTo(iso_country,RU)]
       +- FileScan parquet [ident#58,type#59,name#60,elevation_ft#61,continent#62,iso_country#63,iso_region#64,municipality#65,gps_code#66,iata_code#67,local_code#68,coordinates#69] Batched: true, DataFilters: [isnotnull(iso_country#63), (iso_country#63 = RU)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [IsNotNull(iso_country), EqualTo(iso_country,RU)], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
   */


  /** Schema evolution */
  case class AppleBase1(size: Int, color: String)
  case class PriceApple(size: Int, color: String, price: Double)

//  Seq(AppleBase1(1, "green"))
//    .toDS
//    .write
//    .mode(SaveMode.Append)
//    .parquet("src/main/resources/l_6/apples-10")

//  Seq(PriceApple(1, "green", 2.0))
//    .toDS
//    .write
//    .mode(SaveMode.Append)
//    .parquet("src/main/resources/l_6/apples-10")

  /**
   * !!! несмотря на то, что файлы имеют разную схему - Spark, ВОЗМОЖНО, корректно прочитает файлы, используя обобщенную схему
   * Перед чтением данных читается 1 произвольный паркет файл и из него читается схема (в нашем случае можно потерять колонку price)
   *
   * это работает только при ДОБАВЛЕНИИ в схему новых колонок
   */
  val applesDfNoMerge: DataFrame =
    spark
      .read
      .parquet("src/main/resources/l_6/apples-10")

  applesDfNoMerge.show()

  /**
   * !!! spark.sql.parquet.mergeSchema = true - будут прочитаны схемы всех паркет файлов и создана ОБЪЕДИНЕННАЯ схема
   * false - будет прочитан 1 случайный паркет файл и из него будет прочитана схема
   */
  spark.conf.set("spark.sql.parquet.mergeSchema", "true")

  val applesDfMerge: DataFrame =
    spark
      .read
      .parquet("src/main/resources/l_6/apples-10")

  applesDfMerge.show()


  /** если записать новый файл, ИЗМЕНИВ тип уже существующей колонки - получим ошибку */
  case class AppleBase2(size: Int, color: String)
  case class AppleChanged(size: Double)

//  List(AppleBase2(1, "green"))
//    .toDS
//    .write
//    .mode(SaveMode.Append)
//    .parquet("src/main/resources/l_6/apples-11")

//  List(AppleChanged(3.0))
//    .toDS
//    .write
//    .mode(SaveMode.Append)
//    .parquet("src/main/resources/l_6/apples-11")

  /**
   * org.apache.spark.SparkException: [CANNOT_MERGE_SCHEMAS] Failed merging schemas:
   * Initial schema:
   * "STRUCT<size: DOUBLE>"
   * Schema that cannot be merged with the initial schema:
   * "STRUCT<size: INT, color: STRING>"
   */
//  val changedParquetDf: DataFrame =
//    spark
//      .read
//      .parquet("src/main/resources/l_6/apples-11")

  /** !!! печать всех доступных опций для parquet */
  /** scala.ScalaReflectionException: object org.apache.spark.sql.hive.HiveUtils not found -> build.sbt "spark-hive" */
  val optionDf: Dataset[Row] =
    spark
      .sql("SET -v")
      .filter($"key" contains "parquet")

  optionDf.show(200, truncate = false)


  /** Сравнение скорости записи + обработки запросов для разных форматов */
//  spark.time {
//    1 to 40 foreach { _ =>
//      airportsDf
//        .repartition(1)
//        .write
//        .mode(SaveMode.Append)
//        .parquet("src/main/resources/l_6/speed-test-12/parquet")
//    }
//  } // 14068 ms
//
//  spark.time {
//    1 to 40 foreach { _ =>
//      airportsDf
//        .repartition(1)
//        .write
//        .mode(SaveMode.Append)
//        .orc("src/main/resources/l_6/speed-test-12/orc")
//    }
//  } // 14869 ms
//
//  spark.time {
//    1 to 40 foreach { _ =>
//      airportsDf
//        .repartition(1)
//        .write
//        .mode(SaveMode.Append)
//        .json("src/main/resources/l_6/speed-test-12/json")
//    }
//  } // 10269 ms

  println()

  case class DatasetFormat[T](ds: Dataset[T], format: String)

  val datasets: Seq[DatasetFormat[Row]] =
    Seq(
      DatasetFormat(spark.read.parquet("src/main/resources/l_6/speed-test-12/parquet"), "parquet"),
      DatasetFormat(spark.read.orc("src/main/resources/l_6/speed-test-12/orc"), "orc"),
      DatasetFormat(spark.read.json("src/main/resources/l_6/speed-test-12/json"), "json")
    )

//  datasets.foreach { dsf =>
//    println(s"Running ${dsf.format}: ")
//
//    spark.time {
//      val count: Long =
//        dsf
//          .ds
//          .filter($"iso_country" === "RU" and $"elevation_ft" > 300)
//          .count()
//
//      println(count)
//    }
//    println()
//  }
  /*
    Running parquet:
    16400
    Time taken: 521 ms

    Running orc:
    16400
    Time taken: 516 ms

    Running json:
    16400
    Time taken: 1272 ms
   */

//  datasets.foreach { dsf =>
//    println(s"Running ${dsf.format}")
//    spark.time { dsf.ds.count() }
//    println()
//  }
  /*
    Running parquet
    Time taken: 171 ms

    Running orc
    Time taken: 228 ms

    Running json
    Time taken: 1245 ms
   */

  /** json лучше хранить в паркете */
//  airportsDf
//    .select(to_json(struct(col("*"))).alias("value"))
//    .write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/l_6/json2parquet-13")

  val parquetWithJsonDf: DataFrame =
  spark
    .read
    .parquet("src/main/resources/l_6/json2parquet-13")

  parquetWithJsonDf.show(3, truncate = false)


  println(sc.uiWebUrl)
  Thread.sleep(1_000_000)

  spark.stop()
}
