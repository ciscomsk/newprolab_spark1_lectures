package l_6

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object Datasource_2 extends App {
  // не работает в Spark 3.4.0
//  Logger
//    .getLogger("org")
//    .setLevel(Level.OFF)

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_6")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

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

  /** !!! Отключение записи crc файлов  */
  val hadoopConf: Configuration = sc.hadoopConfiguration
  FileSystem.get(hadoopConf).setWriteChecksum(false)

//  airportsDf
//    .repartition(1)
//    .write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/l_6/airports-8.parquet")

  val parquetDf: DataFrame =
    spark
      .read
      .parquet("src/main/resources/l_6/airports-8.parquet")

  parquetDf.printSchema()
  println(parquetDf.rdd.getNumPartitions)
  println()

  /**
   *  В каждой row group для каждой колонки рассчитываются min/max значения =>
   *  фильтр будет спущен в PushedFilters.
   */
  parquetDf
    .filter($"iso_country" === "RU")
    .explain()
  /*
    == Physical Plan ==
    *(1) Filter (isnotnull(iso_country#87) AND (iso_country#87 = RU))
    +- *(1) ColumnarToRow
       // PushedFilters: [IsNotNull(iso_country), EqualTo(iso_country,RU)]
       +- FileScan parquet [ident#82,type#83,name#84,elevation_ft#85,continent#86,iso_country#87,iso_region#88,municipality#89,gps_code#90,iata_code#91,local_code#92,coordinates#93] Batched: true, DataFilters: [isnotnull(iso_country#87), (iso_country#87 = RU)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [IsNotNull(iso_country), EqualTo(iso_country,RU)], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
   */

  /** Schema evolution */
  case class AppleBase(size: Int, color: String)
  case class PriceApple(size: Int, color: String, price: Double)

//  List(AppleBase(1, "green"))
//    .toDS
//    .write
//    .mode(SaveMode.Append)
//    .parquet("src/main/resources/l_6/apples-9")

//  List(PriceApple(1, "green", 2.0))
//    .toDS
//    .write
//    .mode(SaveMode.Append)
//    .parquet("src/main/resources/l_6/apples-9")

  /**
   * !!! Несмотря на то, что файлы имеют разную схему - Spark ВОЗМОЖНО корректно прочитает файлы, используя обобщенную схему
   * читается 1 произвольный паркет файл - и из него выводится схема (в нашем случае могли потерять колонку price)
   *
   * Это работает только при ДОБАВЛЕНИИ в схему новых колонок
   */
  val applesDf: DataFrame =
    spark
      .read
      .parquet("src/main/resources/l_6/apples-9")

  applesDf.show()

  /**
   * !!! Читает все паркет файл и выводит объединенную схему
   * без этой опции - будет прочитан 1 случайный паркет файл и из него будет выведена схема
   */
  spark.conf.set("spark.sql.parquet.mergeSchema", "true")


  /** Если записать новый файл, ИЗМЕНИВ тип уже существующей колонки - получим ошибку */
//  case class AppleBase(size: Int, color: String)
  case class AppleChanged(size: Double)

//  List(AppleBase(1, "green"))
//    .toDS
//    .write
//    .mode(SaveMode.Append)
//    .parquet("src/main/resources/l_6/apples-10")

//  List(AppleChanged(3.0))
//    .toDS
//    .write
//    .mode(SaveMode.Append)
//    .parquet("src/main/resources/l_6/apples-10")

  // err - Failed merging schema. Failed to merge fields 'size' and 'size'. Failed to merge incompatible data types int and double
//  val changedParquetDf: DataFrame =
//    spark
//      .read
//      .parquet("src/main/resources/l_6/apples-10")

  /** !!! Вывод всех доступных опций для parquet */
  val optionDf: Dataset[Row] =
    spark
      .sql("SET -v")
      .filter($"key" contains "parquet")

  optionDf.show(false)


  /** Сравнение скорости записи + обработки запросов для разных форматов */
  spark.time {
    1 to 40 foreach { _ =>
//      airportsDf
//        .repartition(1)
//        .write
//        .mode(SaveMode.Append)
//        .parquet("src/main/resources/l_6/speed-test-11/parquet")
      // 15938 ms

//      airportsDf
//        .repartition(1)
//        .write
//        .mode(SaveMode.Append)
//        .orc("src/main/resources/l_6/speed-test-11/orc")
      // 15371 ms

//      airportsDf
//        .repartition(1)
//        .write
//        .mode(SaveMode.Append)
//        .json("src/main/resources/l_6/speed-test-11/json")
      // 11429 ms
    }
  }
  println()

  case class DatasetFormat[T](ds: Dataset[T], format: String)

  val datasets: List[DatasetFormat[Row]] =
    List(
      DatasetFormat(spark.read.parquet("src/main/resources/l_6/speed-test-11/parquet"), "parquet"),
      DatasetFormat(spark.read.orc("src/main/resources/l_6/speed-test-11/orc"), "orc"),
      DatasetFormat(spark.read.json("src/main/resources/l_6/speed-test-11/json"), "json")
    )

  datasets.foreach { el =>
    println(s"Running ${el.format}: ")

    spark.time {
      val count: Long =
        el
          .ds
          .filter($"iso_country" === "RU" and $"elevation_ft" > 300)
          .count()

      println(count)
    }
    println()
  }
  /*
    Running parquet:
    16400
    Time taken: 299 ms

    Running orc:
    16400
    Time taken: 463 ms

    Running json:
    16810
    Time taken: 982 ms
   */

  datasets.foreach { el =>
    println(s"Running ${el.format}")
    spark.time { el.ds.count() }
    println()
  }
  /*
    Running parquet
    Time taken: 98 ms

    Running orc
    Time taken: 86 ms

    Running json
    Time taken: 828 ms
   */

  /** json лучше хранить в паркете */
  airportsDf
    .select(to_json(struct(col("*"))).alias("value"))
    .write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/l_6/json2parquet-12")


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
