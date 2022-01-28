package l_6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{current_date, current_timestamp, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Spark 2.4.8 | Java 8 */
object Datasource_3 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_6")
    .getOrCreate

  import spark.implicits._

  val csvOption: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame = spark
    .read
    .options(csvOption)
    .csv("src/main/resources/l_3/airport-codes.csv")

  /**
   * Запуск в докере:
   *
   * // Elastic + Kibana
   * docker network create elastic
   * docker pull docker.elastic.co/elasticsearch/elasticsearch:7.16.3
   * docker run --name es01-test --net elastic -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.16.3
   *
   * docker pull docker.elastic.co/kibana/kibana:7.16.3
   * docker run --name kib01-test --net elastic -p 5601:5601 -e "ELASTICSEARCH_HOSTS=http://es01-test:9200" docker.elastic.co/kibana/kibana:7.16.3
   *
   * // Elastic
   * docker pull docker.elastic.co/elasticsearch/elasticsearch:7.16.3
   * docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.16.3
   *
   *
   * docker ps
   * docker stop <containder_id>
   */

  val esOptions: Map[String, String] = Map(
    /** Адрес узла для подключения. */
    "es.nodes" -> "localhost:9200",
    /** !!! Использовать всегда - для быстрой записи (меньше загружает cpu на дата нодах). */
    "es.batch.write.refresh" -> "false",
    /**
     * Использовать для тестов. Не для продового кластера.
     * Не использовать дискавери (поиск) других узлов кластера. Писать только в указанную ноду.
     */
    "es.nodes.wan.only" -> "true"
  )

//  airportsDf
    /** Колонка ts из elastic template. */
//    .withColumn("ts", current_timestamp)
    /** date - будет использована как часть имени индекса => airports-{date} */
//    .withColumn("date", current_date)
//    .withColumn("foo", lit("foo"))
//    .write
//    .format("es")
//    .options(esOptions)
    /** date - ссылка на колонку с датой. */
//    .save("airports-{date}/_doc")

  val esDf: DataFrame = spark
    .read
    .format("es")
    .options(esOptions)
    /** Паттерн для чтения определенных индексов. */
    .load("airports-*")

  esDf.printSchema()
  esDf.show(1, 200, vertical = true)

  /**
   * Количество партиций в DF совпадает с общим числом шардов индексов, которые указаны в load.
   * У нас 1 индекс и 1 шард => 1 партиция.
   */
  println(esDf.rdd.getNumPartitions)
  println()

  /** К применяемым фильтрам применяется оптимизация filter pushdown. */
  esDf
    .filter('iso_region contains "RU")  // contains - полнотекстовый поиск
    .explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter Contains('iso_region, RU)
    +- Relation[continent#34,coordinates#35,date#36L,elevation_ft#37L,foo#38,gps_code#39,iata_code#40,ident#41,iso_country#42,iso_region#43,local_code#44,municipality#45,name#46,ts#47,type#48] ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.nodes.wan.only -> true, es.resource -> airports-*, es.batch.write.refresh -> false),org.apache.spark.sql.SQLContext@66863941,None)

    == Analyzed Logical Plan ==
    continent: string, coordinates: string, date: bigint, elevation_ft: bigint, foo: string, gps_code: string, iata_code: string, ident: string, iso_country: string, iso_region: string, local_code: string, municipality: string, name: string, ts: timestamp, type: string
    Filter Contains(iso_region#43, RU)
    +- Relation[continent#34,coordinates#35,date#36L,elevation_ft#37L,foo#38,gps_code#39,iata_code#40,ident#41,iso_country#42,iso_region#43,local_code#44,municipality#45,name#46,ts#47,type#48] ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.nodes.wan.only -> true, es.resource -> airports-*, es.batch.write.refresh -> false),org.apache.spark.sql.SQLContext@66863941,None)

    == Optimized Logical Plan ==
    Filter (isnotnull(iso_region#43) && Contains(iso_region#43, RU))
    +- Relation[continent#34,coordinates#35,date#36L,elevation_ft#37L,foo#38,gps_code#39,iata_code#40,ident#41,iso_country#42,iso_region#43,local_code#44,municipality#45,name#46,ts#47,type#48] ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.nodes.wan.only -> true, es.resource -> airports-*, es.batch.write.refresh -> false),org.apache.spark.sql.SQLContext@66863941,None)

    == Physical Plan ==
    *(1) Filter (isnotnull(iso_region#43) && Contains(iso_region#43, RU))
    // PushedFilters: [IsNotNull(iso_region), StringContains(iso_region,RU)]
    +- *(1) Scan ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.nodes.wan.only -> true, es.resource -> airports-*, es.batch.write.refresh -> false),org.apache.spark.sql.SQLContext@66863941,None) [continent#34,coordinates#35,date#36L,elevation_ft#37L,foo#38,gps_code#39,iata_code#40,ident#41,iso_country#42,iso_region#43,local_code#44,municipality#45,name#46,ts#47,type#48] PushedFilters: [IsNotNull(iso_region), StringContains(iso_region,RU)], ReadSchema: struct<continent:string,coordinates:string,date:bigint,elevation_ft:bigint,foo:string,gps_code:st...
   */

  esDf
    .filter(
      'ts between (
        lit("2020-06-01 16:57:30.000").cast("timestamp"),
        lit("2020-06-01 16:59:30.000").cast("timestamp")
      )
    )
    .explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter (('ts >= cast(2020-06-01 16:57:30.000 as timestamp)) && ('ts <= cast(2020-06-01 16:59:30.000 as timestamp)))
    +- Relation[continent#34,coordinates#35,date#36L,elevation_ft#37L,foo#38,gps_code#39,iata_code#40,ident#41,iso_country#42,iso_region#43,local_code#44,municipality#45,name#46,ts#47,type#48] ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.nodes.wan.only -> true, es.resource -> airports-*, es.batch.write.refresh -> false),org.apache.spark.sql.SQLContext@66863941,None)

    == Analyzed Logical Plan ==
    continent: string, coordinates: string, date: bigint, elevation_ft: bigint, foo: string, gps_code: string, iata_code: string, ident: string, iso_country: string, iso_region: string, local_code: string, municipality: string, name: string, ts: timestamp, type: string
    Filter ((ts#47 >= cast(2020-06-01 16:57:30.000 as timestamp)) && (ts#47 <= cast(2020-06-01 16:59:30.000 as timestamp)))
    +- Relation[continent#34,coordinates#35,date#36L,elevation_ft#37L,foo#38,gps_code#39,iata_code#40,ident#41,iso_country#42,iso_region#43,local_code#44,municipality#45,name#46,ts#47,type#48] ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.nodes.wan.only -> true, es.resource -> airports-*, es.batch.write.refresh -> false),org.apache.spark.sql.SQLContext@66863941,None)

    == Optimized Logical Plan ==
    Filter ((isnotnull(ts#47) && (ts#47 >= 1591019850000000)) && (ts#47 <= 1591019970000000))
    +- Relation[continent#34,coordinates#35,date#36L,elevation_ft#37L,foo#38,gps_code#39,iata_code#40,ident#41,iso_country#42,iso_region#43,local_code#44,municipality#45,name#46,ts#47,type#48] ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.nodes.wan.only -> true, es.resource -> airports-*, es.batch.write.refresh -> false),org.apache.spark.sql.SQLContext@66863941,None)

    == Physical Plan ==
    *(1) Filter ((isnotnull(ts#47) && (ts#47 >= 1591019850000000)) && (ts#47 <= 1591019970000000))
    // PushedFilters: [IsNotNull(ts), GreaterThanOrEqual(ts,2020-06-01 16:57:30.0), LessThanOrEqual(ts,2020-06-01 16:59...
    +- *(1) Scan ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.nodes.wan.only -> true, es.resource -> airports-*, es.batch.write.refresh -> false),org.apache.spark.sql.SQLContext@66863941,None) [continent#34,coordinates#35,date#36L,elevation_ft#37L,foo#38,gps_code#39,iata_code#40,ident#41,iso_country#42,iso_region#43,local_code#44,municipality#45,name#46,ts#47,type#48] PushedFilters: [IsNotNull(ts), GreaterThanOrEqual(ts,2020-06-01 16:57:30.0), LessThanOrEqual(ts,2020-06-01 16:59..., ReadSchema: struct<continent:string,coordinates:string,date:bigint,elevation_ft:bigint,foo:string,gps_code:st...
   */

  Thread.sleep(1000000)
}
