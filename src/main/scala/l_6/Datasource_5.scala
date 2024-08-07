package l_6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{monotonically_increasing_id, rand, round, spark_partition_id}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Datasource_5 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_6")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  val csvOption: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOption)
      .csv("src/main/resources/l_3/airport-codes.csv")

  /**
   * Запуск в докере:
   *
   * docker run --rm -p 5432:5432 --name test_postgres -e POSTGRES_PASSWORD=12345 postgres:latest
   * docker exec -it test_postgres psql -U postgres
   *
   * CREATE DATABASE airports;
   * \c airports; - подключиться к БД airports
   * CREATE TABLE IF NOT EXISTS codes (ident VARCHAR (100) PRIMARY KEY,type VARCHAR (100),name VARCHAR (100),elevation_ft INTEGER,continent VARCHAR (100),iso_country VARCHAR (100),iso_region VARCHAR (100),municipality VARCHAR (100),gps_code VARCHAR (100),iata_code VARCHAR (100),local_code VARCHAR (100),coordinates VARCHAR (100));
   * SELECT * from codes;
   *
   * \d codes; - describe
   *
   * CREATE TABLE IF NOT EXISTS codes_x (ident VARCHAR (100) PRIMARY KEY,type VARCHAR (100),name VARCHAR (100),elevation_ft INTEGER,continent VARCHAR (100),iso_country VARCHAR (100),iso_region VARCHAR (100),municipality VARCHAR (100),gps_code VARCHAR (100),iata_code VARCHAR (100),local_code VARCHAR (100),coordinates VARCHAR (100),id INTEGER);
   * SELECT * from codes_x;
   */

  val typesMap: Map[String, String] = Map("string" -> "VARCHAR (100)", "int" -> "INTEGER")
  val primaryKey: String = "ident"

  val ddlColumns: String =
    airportsDf
      .schema
      .fields
      .map { field =>
        val fieldType: String = field.dataType.simpleString

        if (field.name == primaryKey) s"${field.name} ${typesMap(fieldType)} PRIMARY KEY"
        else s"${field.name} ${typesMap(fieldType)}"
      }
      .mkString(",")

  val ddlQuery: String = s"CREATE TABLE IF NOT EXISTS codes ($ddlColumns);"
  println()
  println(ddlQuery)
  // CREATE TABLE IF NOT EXISTS codes (ident VARCHAR (100) PRIMARY KEY,type VARCHAR (100),name VARCHAR (100),elevation_ft INTEGER,continent VARCHAR (100),iso_country VARCHAR (100),iso_region VARCHAR (100),municipality VARCHAR (100),gps_code VARCHAR (100),iata_code VARCHAR (100),local_code VARCHAR (100),coordinates VARCHAR (100));
  println()

  val jdbcUrl: String = "jdbc:postgresql://localhost/airports?user=postgres&password=12345"

//  airportsDf
//    .write
//    .format("jdbc")
//    .option("url", jdbcUrl)
//    .option("dbtable", "codes")
//    .mode(SaveMode.Append)
//    .save()

  val postgesDf: DataFrame =
    spark
      .read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "codes")
      .load()

  postgesDf.printSchema()
  postgesDf.show(1, 200, vertical = true)
  /** !!! по умолчанию чтение производится в 1 партицию */
  println(postgesDf.rdd.getNumPartitions)
  println()


  /** Чтение в несколько партиций */
  val ddlColumnPart: String = s"$ddlColumns,id INTEGER"
  val ddlQueryPart = s"CREATE TABLE IF NOT EXISTS codes_x ($ddlColumnPart);"
  println(ddlQueryPart)
  // CREATE TABLE IF NOT EXISTS codes_x (ident VARCHAR (100) PRIMARY KEY,type VARCHAR (100),name VARCHAR (100),elevation_ft INTEGER,continent VARCHAR (100),iso_country VARCHAR (100),iso_region VARCHAR (100),municipality VARCHAR (100),gps_code VARCHAR (100),iata_code VARCHAR (100),local_code VARCHAR (100),coordinates VARCHAR (100),id INTEGER);
  println()

//  airportsDf
//    .withColumn("id", round(rand() * 10000).cast("int"))
//    .write
//    .format("jdbc")
//    .option("url", jdbcUrl)
//    .option("dbtable", "codes_x")
//    .mode(SaveMode.Append)
//    .save()

  val postgresPartDf: DataFrame =
    spark
      .read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "codes_x")
      /** колонка партиционирования */
      .option("partitionColumn", "id")
      /** lowerBound/upperBound - задаются вручную */
      .option("lowerBound", "0")
      /** если ошибиться в этом параметре - данные в полученном датафрейме будут перекошены */
      .option("upperBound", "10000")
//      .option("upperBound", "100000")
      .option("numPartitions", "200")
      .load()

  postgresPartDf.printSchema()
  postgresPartDf.show(1, 200, vertical = true)
  println(postgresPartDf.rdd.getNumPartitions) // = 200
  println()

  /** Проверка распределения данных по партициям */
  postgresPartDf
    .groupBy(spark_partition_id())
    .count()
    .show(200, truncate = false)

  /**
   * monotonically_increasing_id - генерирует монотонно возрастающий счетчик
   * счетчик неразрывен в пределах каждой партиции, между партициями - большие разрывы
   */
  monotonically_increasing_id()


  println(sc.uiWebUrl)
  Thread.sleep(1_000_000)

  spark.stop()
}
