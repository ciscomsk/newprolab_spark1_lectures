name := "lectures"

version := "0.1"

//scalaVersion := "2.12.15"
scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "3.2.0" % Provided,

//  "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.2.0" % Provided, // Provided - зависимость не будет включена при сборке fat jar (плагин assembly)

  // !!! works only with Spark 2.4.8
//  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.16.3",

  // err - with Spark3 (Java 11) - java.lang.NoClassDefFoundError: org/spark_project/guava/cache/CacheLoader
  // err - 7.15.1 | 7.16.3
//  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.16.3",

  // err - with Spark3 (Java8/11)java.lang.NoClassDefFoundError: org/apache/spark/network/shuffle/RetryingBlockFetcher$BlockFetchStarter
  // err - 7.15.1/2 | 7.16.0/1/2/3 | 8.0.0-rc1
//  "org.elasticsearch" %% "elasticsearch-spark-30" % "7.16.3",

//  "com.datastax.spark" %% "spark-cassandra-connector" % "3.1.0", // works only with joda-time + нет под scala 2.13
//  "joda-time" % "joda-time" % "2.10.12",
//  "joda-time" % "joda-time" % "2.10.13",  // новая версия - не тестировал
  /** java.lang.NoClassDefFoundError: jnr/posix/POSIXHandler - и работает дальше. */

  /** Похоже нужен файл конфигурации - очень много логов. */
//  "ch.qos.logback" % "logback-classic" % "1.2.6",

  /**
   * Без файла конфигурации:
   * Пример: 21/10/27 14:08:38 INFO CassandraConnector: Connected to Cassandra cluster.
   *
   * С log4j.properties:
   * Пример: 14:09:18,328 INFO  com.datastax.spark.connector.cql.CassandraConnector           - Connected to Cassandra cluster.
   */
  "org.slf4j" % "slf4j-log4j12" % "1.7.33",

  "org.postgresql" % "postgresql" % "42.3.1",

  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0",

  // Scala 2.13
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",


  "org.scalatest" %% "scalatest" % "3.2.11" % Test
)