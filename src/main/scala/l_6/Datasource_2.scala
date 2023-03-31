package l_6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object Datasource_2 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.OFF)

  val spark: SparkSession =
    SparkSession
      .builder
      .master("local[*]")
      .appName("l_6")
      .getOrCreate

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  val csvOption: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOption)
      .csv("src/main/resources/l_3/airport-codes.csv")

  airportsDf.printSchema()
  println(airportsDf.count())
  println()

  /** !!! Не записывать crc файлы - не работает.  */
//  sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")

//  airportsDf
//    .repartition(1)
//    .write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/l_6/airports-8.parquet")

  val parquetDf: DataFrame = spark
    .read
    .parquet("src/main/resources/l_6/airports-8.parquet")

  parquetDf.printSchema()
  println(parquetDf.rdd.getNumPartitions)

  /**
   *  В каждой row group для каждой колонки рассчитываются min/max значения =>
   *  фильтр будет спущен в PushedFilters.
   */
  parquetDf
    .filter('iso_country === "RU")
    .explain
  /*
    == Physical Plan ==
    *(1) Filter (isnotnull(iso_country#86) AND (iso_country#86 = RU))
    +- *(1) ColumnarToRow
       // PushedFilters: [IsNotNull(iso_country), EqualTo(iso_country,RU)]
       +- FileScan parquet [ident#81,type#82,name#83,elevation_ft#84,continent#85,iso_country#86,iso_region#87,municipality#88,gps_code#89,iata_code#90,local_code#91,coordinates#92] Batched: true, DataFilters: [isnotnull(iso_country#86), (iso_country#86 = RU)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [IsNotNull(iso_country), EqualTo(iso_country,RU)], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
   */

  /** Schema evolution. */
  case class AppleBase(size: Int, color: String)

//  List(AppleBase(1, "green"))
//    .toDS
//    .write
//    .mode(SaveMode.Append)
//    .parquet("src/main/resources/l_6/apples-9")
////    .parquet("src/main/resources/l_6/apples-10")

  case class PriceApple(size: Int, color: String, price: Double)

//  List(PriceApple(1, "green", 2.0))
//    .toDS
//    .write
//    .mode(SaveMode.Append)
//    .parquet("src/main/resources/l_6/apples-9")

  /**
   * !!! Несмотря на то, что файлы имеют разную схему - Spark возможно корректно прочитает файлы, используя обобщенную схему.
   * Читается 1 произвольный паркет файл - и из него выводится схема (в нашем случае могли потерять колонку price).
   *
   * Это работает только при ДОБАВЛЕНИИ в схему новых колонок.
   */
  val applesDf: DataFrame = spark
    .read
    .parquet("src/main/resources/l_6/apples-9")

  applesDf.show()

  /**
   * !!! Читает все паркет файл и выводит объединенную схему.
   * Без этой опции - будет прочитан 1 случайный паркет файл и из него будет выведена схема.
   */
  spark.conf.set("spark.sql.parquet.mergeSchema", "true")


  /** Если записать новый файл, ИЗМЕНИВ тип уже существующей колонки - получим ошибку. */
  case class AppleChanged(size: Double)

//  List(AppleChanged(3.0))
//    .toDS
//    .write
//    .mode(SaveMode.Append)
//    .parquet("src/main/resources/l_6/apples-10")

  // err - Failed merging schema. Failed to merge fields 'size' and 'size'. Failed to merge incompatible data types int and double
//  val changedParquetDf: DataFrame = spark
//    .read
//    .parquet("src/main/resources/l_6/apples-10")

  /** !!! Вывод всех доступных опций для parquet. */
  val optionDf: Dataset[Row] = spark
    .sql("SET -v")
    .filter('key contains "parquet")

  optionDf.show(false)


  /** Сравнение скорости обработки запросов. */
//  spark.time {
//    1 to 40 foreach { _ =>
//      airportsDf
//        .repartition(1)
//        .write
//        .mode(SaveMode.Append)
//        .parquet("src/main/resources/l_6/speed-test-11/parquet")
      // 16585 ms

//      airportsDf
//        .repartition(1)
//        .write
//        .mode(SaveMode.Append)
//        .json("src/main/resources/l_6/speed-test-11/json")
      // 11016 ms

//      airportsDf
//        .repartition(1)
//        .write
//        .mode(SaveMode.Append)
//        .orc("src/main/resources/l_6/speed-test-11/orc")
      // 15957 ms
//    }
//  }

  case class DatasetFormat[T](ds: Dataset[T], format: String)

  val datasets: List[DatasetFormat[Row]] =
    DatasetFormat(spark.read.orc("src/main/resources/l_6/speed-test-11/orc"), "orc") ::
    DatasetFormat(spark.read.parquet("src/main/resources/l_6/speed-test-11/parquet"), "parquet") ::
    DatasetFormat(spark.read.json("src/main/resources/l_6/speed-test-11/json"), "json") ::
    Nil

  datasets.foreach { el =>
    println(s"Running ${el.format}")

    spark.time {
      println(el.ds.filter($"iso_country" === "RU" and $"elevation_ft" > 300).count)
    }
    println()
  }
  /*
    Running orc
    16400
    Time taken: 470 ms

    Running parquet
    16400
    Time taken: 390 ms

    Running json
    16400
    Time taken: 1727 ms
   */

  datasets.foreach { el =>
    println(s"Running ${el.format}")
    spark.time { el.ds.count }
    println()
  }
  /*
    Running orc
    Time taken: 100 ms

    Running parquet
    Time taken: 84 ms

    Running json
    Time taken: 923 ms
   */

  /** json лучше хранить в паркете. */
//  airportsDf
//    .select(to_json(struct(col("*"))).alias("value"))
//    .write
//    .parquet("src/main/resources/l_6/json2parquet-12")

  Thread.sleep(1000000)
}
