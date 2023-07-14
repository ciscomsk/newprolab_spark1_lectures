package l_6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Datasource_4 extends App {
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

  val csvOption: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOption)
      .csv("src/main/resources/l_3/airport-codes.csv")


  /**
   * Запуск в докере:
   *
   * Запуск инстанса:
   * docker run --rm --name cass -p 9042:9042 -e CASSANDRA_BROADCAST_ADDRESS=127.0.0.1 cassandra:latest
   *
   * Подключение к cassandra:
   * docker exec -it cass cqlsh (cqlsh - /opt/cassandra/bin/)
   *
   * CREATE KEYSPACE IF NOT EXISTS airports WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};
   * SimpleStrategy - кластер внутри одного дата-центра, быстрее чем гео-распределенный, но масштабировать
   * на несколько дата центров не получится
   *
   * DESCRIBE KEYSPACE airports; - покажет команду, которая была использована при создании кейспейса
   *
   * CREATE TABLE IF NOT EXISTS airports.codes (ident text PRIMARY KEY,type text,name text,elevation_ft int,continent text,iso_country text,iso_region text,municipality text,gps_code text,iata_code text,local_code text,coordinates text);
   * SELECT * FROM airports.codes LIMIT 10;
   * SELECT ident, continent, iso_country, name FROM airports.codes LIMIT 10;
   *
   * // После записи датафрейма
   * SELECT ident, continent, iso_country, name FROM airports.codes WHERE ident = 'UGB' LIMIT 10;
   * Отработает очень быстро на любом объеме данных - т.к. мы точно знаем  на каком хосте данные,
   * ident - PARTITION KEY, если бы дополнительно был CLUSTERING KEY - знали бы сортировку внутри партиции
   *
   * SELECT ident, continent, iso_country, name FROM airports.codes WHERE continent = 'NA' LIMIT 10;
   * err - InvalidRequest: Error from server: code2200 [Invalid query] message="Cannot execute this query as it might
   * involve data filtering and thus may have unpredictable performance. If you want to execute this query despite
   * the performance unpredictability, use ALLOW FILTERING"
   *
   * ALLOW FILTERING - позволит выполнить запрос с фильтрацией не по partition key
   * SELECT ident, continent, iso_country, name FROM airports.codes WHERE continent = 'NA' LIMIT 10 ALLOW FILTERING;
   * Может приводить к очень долгому ответу/ошибке таймаута - FULL TABLE SCAN
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

  /** Можно указать несколько адресов нод - остальные будут найдены через механизм дискавери */
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

  airportsDf
    .write
    .format("org.apache.spark.sql.cassandra")
    .mode(SaveMode.Append)
    .options(tableOpts)
    .save()

//  val cassandraDf: DataFrame =
//    spark
//      .read
//      .format("org.apache.spark.sql.cassandra")
//      .options(tableOpts)
//      .load()

//  cassandraDf.show(1, 200, vertical = true)

//  cassandraDf
//    .filter($"ident" === "22MV")
//    .explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter ('ident = 22MV)
    +- RelationV2[ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51] codes

    == Analyzed Logical Plan ==
    ident: string, continent: string, coordinates: string, elevation_ft: int, gps_code: string, iata_code: string, iso_country: string, iso_region: string, local_code: string, municipality: string, name: string, type: string
    Filter (ident#40 = 22MV)
    +- RelationV2[ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51] codes

    == Optimized Logical Plan ==
    RelationV2[ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51] codes

    == Physical Plan ==
    *(1) Project [ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51]
    +- BatchScan[ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51] Cassandra Scan: airports.codes
     - Cassandra Filters: [["ident" = ?, 22MV]]
     - Requested Columns: [ident,continent,coordinates,elevation_ft,gps_code,iata_code,iso_country,iso_region,local_code,municipality,name,type] RuntimeFilters: []
   */

//  spark.time {
//    cassandraDf
//      .filter($"ident" === "22MV")
//      .show(1, 200, vertical = true)
//  }  // 96 ms
//  println()

//  cassandraDf
//    .filter($"iso_region" === "RU")
//    .explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter ('iso_region = RU)
    +- RelationV2[ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51] codes

    == Analyzed Logical Plan ==
    ident: string, continent: string, coordinates: string, elevation_ft: int, gps_code: string, iata_code: string, iso_country: string, iso_region: string, local_code: string, municipality: string, name: string, type: string
    Filter (iso_region#47 = RU)
    +- RelationV2[ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51] codes

    == Optimized Logical Plan ==
    Filter (iso_region#47 = RU)
    +- RelationV2[ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51] codes

    == Physical Plan ==
    *(1) Project [ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51]
    +- *(1) Filter (iso_region#47 = RU)
       +- BatchScan[ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51] Cassandra Scan: airports.codes
     - Cassandra Filters: []
     - Requested Columns: [ident,continent,coordinates,elevation_ft,gps_code,iata_code,iso_country,iso_region,local_code,municipality,name,type] RuntimeFilters: []
   */

//  spark.time {
//    cassandraDf
//      .filter($"iso_region" === "RU")
//      .show(1, 200, vertical = true)
//  }  // 926 ms
//  println()

//  cassandraDf
//    .filter(lower($"iso_region") === "ru")
//    .explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter (lower('iso_region) = ru)
    +- RelationV2[ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51] codes

    == Analyzed Logical Plan ==
    ident: string, continent: string, coordinates: string, elevation_ft: int, gps_code: string, iata_code: string, iso_country: string, iso_region: string, local_code: string, municipality: string, name: string, type: string
    Filter (lower(iso_region#47) = ru)
    +- RelationV2[ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51] codes

    == Optimized Logical Plan ==
    Filter ((lower(iso_region#47) = ru) AND isnotnull(iso_region#47))
    +- RelationV2[ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51] codes

    == Physical Plan ==
    *(1) Project [ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51]
    +- *(1) Filter ((lower(iso_region#47) = ru) AND isnotnull(iso_region#47))
       +- BatchScan[ident#40, continent#41, coordinates#42, elevation_ft#43, gps_code#44, iata_code#45, iso_country#46, iso_region#47, local_code#48, municipality#49, name#50, type#51] Cassandra Scan: airports.codes
     - Cassandra Filters: []
     - Requested Columns: [ident,continent,coordinates,elevation_ft,gps_code,iata_code,iso_country,iso_region,local_code,municipality,name,type] RuntimeFilters: []
   */

//  spark.time {
//    cassandraDf
//      .filter(lower($"iso_region") === "ru")
//      .show(1, 200, vertical = true)
//  }  // 839 ms


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
