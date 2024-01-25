package l_6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Datasource_4 extends App {
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
   * docker run --rm --name cass -p 9042:9042 -e CASSANDRA_BROADCAST_ADDRESS=127.0.0.1 cassandra:latest
   * docker exec -it cass cqlsh (cqlsh - /opt/cassandra/bin/)
   *
   * CREATE KEYSPACE IF NOT EXISTS airports WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};
   * SimpleStrategy - кластер внутри одного дата-центра, быстрее чем гео-распределенный, но масштабировать
   * на несколько дата центров не получится
   *
   * DESCRIBE KEYSPACE airports; - покажет DDL, который был использован при создании кейспейса (с дополнениями)
   *
   * CREATE TABLE IF NOT EXISTS airports.codes (ident text PRIMARY KEY,type text,name text,elevation_ft int,continent text,iso_country text,iso_region text,municipality text,gps_code text,iata_code text,local_code text,coordinates text);
   * SELECT * FROM airports.codes LIMIT 10;
   *
   * // После записи датафрейма
   * SELECT ident, continent, iso_country, name FROM airports.codes LIMIT 10;
   *
   * SELECT ident, continent, iso_country, name FROM airports.codes WHERE ident = 'UGB' LIMIT 10;
   * запрос отработает очень быстро на любом объеме данных - т.к. мы точно знаем  на каком хосте данные,
   * ident - PARTITION KEY, если бы дополнительно был CLUSTERING KEY - знали бы сортировку внутри партиции
   *
   * SELECT ident, continent, iso_country, name FROM airports.codes WHERE continent = 'NA' LIMIT 10;
   * err - InvalidRequest: Error from server: code=2200 [Invalid query] message="Cannot execute this query
   * as it might involve data filtering and thus may have unpredictable performance.
   * If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"
   *
   * ALLOW FILTERING - позволит выполнить запрос с фильтрацией не по partition key
   * SELECT ident, continent, iso_country, name FROM airports.codes WHERE continent = 'NA' LIMIT 10 ALLOW FILTERING;
   * запрос на большом объеме данных приведет к очень долгому ответу/ошибке по таймауту - FULL TABLE SCAN
   */

  val typesMap: Map[String, String] = Map("string" -> "text", "int" -> "int")
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

  val ddlQuery: String = s"CREATE TABLE IF NOT EXISTS airports.codes ($ddlColumns);"
  println()
  println(ddlQuery)
  // CREATE TABLE IF NOT EXISTS airports.codes (ident text PRIMARY KEY,type text,name text,elevation_ft int,continent text,iso_country text,iso_region text,municipality text,gps_code text,iata_code text,local_code text,coordinates text);
  println()

  /** можно указать несколько адресов нод - остальные будут найдены через механизм дискавери */
  spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")

  /**
   * Сколько хостов должно дать одинаковый ответ, для того, чтобы запрос на запись считался успешным:
   * 1. ANY - любой из хостов подтверждает запись (данные не будут потеряны даже в случае отсутствии хостов,
   * на которые по хэшу должны были попасть эти данные, но они не будут доступные до поднятия хотя бы 1 такого хоста)
   * 2. QUORUM - нужен ответ от кворума хостов
   * 3. ALL - нужен ответ от всех хостов
   */
  spark.conf.set("spark.cassandra.output.consistency.level", "ANY")

  /**
   * Сколько хостов должно дать одинаковый ответ, для того, чтобы запрос на чтение считался успешным:
   * 1. ONE - первый ответ от любого из узлов, считается правильным
   * Самая высокая доступность данных + скорость, самая низкая консистентность
   * 2. QUORUM
   * ...
   */
  spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

  val tableOpts: Map[String, String] = Map("table" -> "codes", "keyspace" -> "airports")

//  airportsDf
//    .write
//    .format("org.apache.spark.sql.cassandra")
//    .mode(SaveMode.Append)
//    .options(tableOpts)
//    .save()

  val cassandraDf: DataFrame =
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(tableOpts)
      .load()

//  cassandraDf.show(1, 200, vertical = true)

  cassandraDf
    .filter(col("ident") === "22WV")
    .explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter ('ident = 22WV)
    +- RelationV2[ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52]  codes

    == Analyzed Logical Plan ==
    ident: string, continent: string, coordinates: string, elevation_ft: int, gps_code: string, iata_code: string, iso_country: string, iso_region: string, local_code: string, municipality: string, name: string, type: string
    Filter (ident#41 = 22WV)
    +- RelationV2[ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52]  codes

    == Optimized Logical Plan ==
    RelationV2[ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52] codes

    == Physical Plan ==
    *(1) Project [ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52]
    +- BatchScan codes[ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52] Cassandra Scan: airports.codes
     - Cassandra Filters: [["ident" = ?, 22WV]]
     - Requested Columns: [ident,continent,coordinates,elevation_ft,gps_code,iata_code,iso_country,iso_region,local_code,municipality,name,type] RuntimeFilters: []
   */

  spark.time {
    cassandraDf
      .filter(col("ident") === "22WV")
      .show(1, 200, vertical = true)
  }  // 419 ms
  println()

  cassandraDf
    .filter(col("iso_country") === "RU")
    .explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter ('iso_country = RU)
    +- RelationV2[ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52]  codes

    == Analyzed Logical Plan ==
    ident: string, continent: string, coordinates: string, elevation_ft: int, gps_code: string, iata_code: string, iso_country: string, iso_region: string, local_code: string, municipality: string, name: string, type: string
    Filter (iso_country#47 = RU)
    +- RelationV2[ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52]  codes

    == Optimized Logical Plan ==
    Filter (iso_country#47 = RU)
    +- RelationV2[ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52] codes

    == Physical Plan ==
    *(1) Project [ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52]
    +- *(1) Filter (iso_country#47 = RU)
       +- BatchScan codes[ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52] Cassandra Scan: airports.codes
     - Cassandra Filters: []
     - Requested Columns: [ident,continent,coordinates,elevation_ft,gps_code,iata_code,iso_country,iso_region,local_code,municipality,name,type] RuntimeFilters: []
   */

  spark.time {
    cassandraDf
      .filter(col("iso_country") === "RU")
      .show(1, 200, vertical = true)
  }  // 205 ms
  println()

  cassandraDf
    .filter(lower(col("iso_country")) === "ru")
    .explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter (lower('iso_country) = ru)
    +- RelationV2[ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52]  codes

    == Analyzed Logical Plan ==
    ident: string, continent: string, coordinates: string, elevation_ft: int, gps_code: string, iata_code: string, iso_country: string, iso_region: string, local_code: string, municipality: string, name: string, type: string
    Filter (lower(iso_country#47) = ru)
    +- RelationV2[ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52]  codes

    == Optimized Logical Plan ==
    Filter (isnotnull(iso_country#47) AND (lower(iso_country#47) = ru))
    +- RelationV2[ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52] codes

    == Physical Plan ==
    *(1) Project [ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52]
    +- *(1) Filter (isnotnull(iso_country#47) AND (lower(iso_country#47) = ru))
       +- BatchScan codes[ident#41, continent#42, coordinates#43, elevation_ft#44, gps_code#45, iata_code#46, iso_country#47, iso_region#48, local_code#49, municipality#50, name#51, type#52] Cassandra Scan: airports.codes
     - Cassandra Filters: []
     - Requested Columns: [ident,continent,coordinates,elevation_ft,gps_code,iata_code,iso_country,iso_region,local_code,municipality,name,type] RuntimeFilters: []
   */

  spark.time {
    cassandraDf
      .filter(lower(col("iso_country")) === "ru")
      .show(1, 200, vertical = true)
  }  // 161 ms


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
