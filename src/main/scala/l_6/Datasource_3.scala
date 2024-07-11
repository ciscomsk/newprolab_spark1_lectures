package l_6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{current_date, current_timestamp, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Datasource_3 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_6")
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

  /**
   * Запуск в докере:
   *
   * docker pull elasticsearch:8.13.4
   * docker pull kibana:8.13.4
   * docker network create elastic
   * docker run --name es-node01 --net elastic -m 16GB -p 9200:9200 -p 9300:9300 -e xpack.security.enabled=false -e discovery.type=single-node elasticsearch:8.13.4
   * docker run --name kib-01 --net elastic -p 5601:5601 -e ELASTICSEARCH_HOSTS=http://es-node01:9200 kibana:8.13.4
   *
   * docker ps
   * docker start 'containder_id'
   * docker stop 'containder_id'
   * docker rm 'containder_id'
   *
   *
   *
   *
   * // Elastic + Kibana
   * // https://hub.docker.com/_/elasticsearch/tags
   * // https://stackoverflow.com/questions/73307726/elasticsearch-error-elasticsearch-exited-unexpectedly-when-trying-to-start-e
   *
   * // https://www.digitalocean.com/community/tutorials/how-to-remove-docker-images-containers-and-volumes
   *
   * docker network create elastic
   * docker pull elasticsearch:8.8.1
   * docker run --name es-node01 --net elastic -m 16GB -p 9200:9200 -p 9300:9300 -e xpack.security.enabled=false -e discovery.type=single-node elasticsearch:8.8.1
   * docker start es-node01
   * -d - optional
   *
   * // !!! не работает - Unable to connect to Elasticsearch
   * docker pull kibana:8.8.1
   * docker run --name kib-01 --net elastic -p 5601:5601 -e ELASTICSEARCH_HOSTS=http://es-node01:9200 kibana:8.8.1
   * -d - optional
   *
   *
   * // Elastic only
   * docker pull elasticsearch:8.8.1
   * docker run -p 9200:9200 -p 9300:9300 -e xpack.security.enabled=false -e discovery.type=single-node elasticsearch:8.8.1
   */

  val esOptions: Map[String, String] =
    Map(
      /** адрес ноды для подключения */
      "es.nodes" -> "localhost:9200",
      /** !!! для быстрой записи (меньше загружает cpu на дата нодах) - использовать всегда */
      "es.batch.write.refresh" -> "false",
      /**
       * не использовать дискавери (поиск) других узлов кластера - писать только в указанную ноду
       * !!! использовать только для тестов - не для продуктового кластера
       */
      "es.nodes.wan.only" -> "true"
    )

//  airportsDf
    /** колонка ts из elastic template */
//    .withColumn("ts", current_timestamp())
    /** date - будет использована как часть имени индекса -> airports-{date} */
//    .withColumn("date", current_date())
//    .withColumn("foo", lit("foo"))  // просто так
//    .write
//    .format("es")
//    .options(esOptions)
    /** {date} - ссылка на колонку с датой */
//    .save("airports-{date}")

  val esDf: DataFrame =
    spark
      .read
      .format("es")
      .options(esOptions)
      /** паттерн для чтения определенных индексов */
      .load("airports-*")

  println()
  esDf.printSchema()
  esDf.show(1, 200, vertical = true)

  /**
   * количество партиций DF равно числу шардов индекса
   * у нас индекс состоит из 1 шарда => 1 партиция
   */
  println(esDf.rdd.getNumPartitions)
  println()

  /** к фильтрам применяется оптимизация predicate pushdown */
  esDf
    .filter($"iso_region" contains "RU") // contains - полнотекстовый поиск
    .explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter Contains('iso_region, RU)
    +- Relation [continent#161,coordinates#162,date#163L,elevation_ft#164L,foo#165,gps_code#166,iata_code#167,ident#168,iso_country#169,iso_region#170,local_code#171,municipality#172,name#173,ts#174,type#175] ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.batch.write.refresh -> false, es.nodes.wan.only -> true, es.resource -> airports-*),org.apache.spark.sql.SQLContext@2812368,None)

    == Analyzed Logical Plan ==
    continent: string, coordinates: string, date: bigint, elevation_ft: bigint, foo: string, gps_code: string, iata_code: string, ident: string, iso_country: string, iso_region: string, local_code: string, municipality: string, name: string, ts: timestamp, type: string
    Filter Contains(iso_region#170, RU)
    +- Relation [continent#161,coordinates#162,date#163L,elevation_ft#164L,foo#165,gps_code#166,iata_code#167,ident#168,iso_country#169,iso_region#170,local_code#171,municipality#172,name#173,ts#174,type#175] ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.batch.write.refresh -> false, es.nodes.wan.only -> true, es.resource -> airports-*),org.apache.spark.sql.SQLContext@2812368,None)

    == Optimized Logical Plan ==
    Filter (isnotnull(iso_region#170) AND Contains(iso_region#170, RU))
    +- Relation [continent#161,coordinates#162,date#163L,elevation_ft#164L,foo#165,gps_code#166,iata_code#167,ident#168,iso_country#169,iso_region#170,local_code#171,municipality#172,name#173,ts#174,type#175] ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.batch.write.refresh -> false, es.nodes.wan.only -> true, es.resource -> airports-*),org.apache.spark.sql.SQLContext@2812368,None)

    == Physical Plan ==
    *(1) Filter (isnotnull(iso_region#170) AND Contains(iso_region#170, RU))
    // PushedFilters: [IsNotNull(iso_region), StringContains(iso_region,RU)]
    +- *(1) Scan ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.batch.write.refresh -> false, es.nodes.wan.only -> true, es.resource -> airports-*),org.apache.spark.sql.SQLContext@2812368,None) [continent#161,coordinates#162,date#163L,elevation_ft#164L,foo#165,gps_code#166,iata_code#167,ident#168,iso_country#169,iso_region#170,local_code#171,municipality#172,name#173,ts#174,type#175] PushedFilters: [IsNotNull(iso_region), StringContains(iso_region,RU)], ReadSchema: struct<continent:string,coordinates:string,date:bigint,elevation_ft:bigint,foo:string,gps_code:st...
   */

  esDf
    .filter(
      $"ts" between (
        lit("2020-06-01 16:57:30.000").cast("timestamp"),
        lit("2020-06-01 16:59:30.000").cast("timestamp")
      )
    )
    .explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter (('ts >= cast(2020-06-01 16:57:30.000 as timestamp)) AND ('ts <= cast(2020-06-01 16:59:30.000 as timestamp)))
    +- Relation [continent#161,coordinates#162,date#163L,elevation_ft#164L,foo#165,gps_code#166,iata_code#167,ident#168,iso_country#169,iso_region#170,local_code#171,municipality#172,name#173,ts#174,type#175] ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.batch.write.refresh -> false, es.nodes.wan.only -> true, es.resource -> airports-*),org.apache.spark.sql.SQLContext@2812368,None)

    == Analyzed Logical Plan ==
    continent: string, coordinates: string, date: bigint, elevation_ft: bigint, foo: string, gps_code: string, iata_code: string, ident: string, iso_country: string, iso_region: string, local_code: string, municipality: string, name: string, ts: timestamp, type: string
    Filter ((ts#174 >= cast(2020-06-01 16:57:30.000 as timestamp)) AND (ts#174 <= cast(2020-06-01 16:59:30.000 as timestamp)))
    +- Relation [continent#161,coordinates#162,date#163L,elevation_ft#164L,foo#165,gps_code#166,iata_code#167,ident#168,iso_country#169,iso_region#170,local_code#171,municipality#172,name#173,ts#174,type#175] ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.batch.write.refresh -> false, es.nodes.wan.only -> true, es.resource -> airports-*),org.apache.spark.sql.SQLContext@2812368,None)

    == Optimized Logical Plan ==
    Filter (isnotnull(ts#174) AND ((ts#174 >= 2020-06-01 16:57:30) AND (ts#174 <= 2020-06-01 16:59:30)))
    +- Relation [continent#161,coordinates#162,date#163L,elevation_ft#164L,foo#165,gps_code#166,iata_code#167,ident#168,iso_country#169,iso_region#170,local_code#171,municipality#172,name#173,ts#174,type#175] ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.batch.write.refresh -> false, es.nodes.wan.only -> true, es.resource -> airports-*),org.apache.spark.sql.SQLContext@2812368,None)

    == Physical Plan ==
    *(1) Filter ((isnotnull(ts#174) AND (ts#174 >= 2020-06-01 16:57:30)) AND (ts#174 <= 2020-06-01 16:59:30))
    // PushedFilters: [IsNotNull(ts), GreaterThanOrEqual(ts,2020-06-01 16:57:30.0), LessThanOrEqual(ts,2020-06-01 16:59...
    +- *(1) Scan ElasticsearchRelation(Map(es.nodes -> localhost:9200, es.batch.write.refresh -> false, es.nodes.wan.only -> true, es.resource -> airports-*),org.apache.spark.sql.SQLContext@2812368,None) [continent#161,coordinates#162,date#163L,elevation_ft#164L,foo#165,gps_code#166,iata_code#167,ident#168,iso_country#169,iso_region#170,local_code#171,municipality#172,name#173,ts#174,type#175] PushedFilters: [IsNotNull(ts), GreaterThanOrEqual(ts,2020-06-01 16:57:30.0), LessThanOrEqual(ts,2020-06-01 16:59..., ReadSchema: struct<continent:string,coordinates:string,date:bigint,elevation_ft:bigint,foo:string,gps_code:st...
   */

  println(sc.uiWebUrl)
  Thread.sleep(1_000_000)

  spark.stop()
}
