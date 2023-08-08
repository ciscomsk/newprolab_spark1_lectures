name := "lectures"

version := "0.1"

scalaVersion := "2.13.11"
val sparkVersion = "3.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,  // Provided - зависимость не будет включена при сборке fat jar (плагин assembly)
//  "org.apache.spark" %% "spark-repl" % "3.4.0" % Provided,  // для 11 лекции

  "org.elasticsearch" %% "elasticsearch-spark-30" % "8.9.0",

//  "com.datastax.spark" %% "spark-cassandra-connector" % "3.4.0" - нет под scala 2.13
//  "joda-time" % "joda-time" % "2.12.5",  // для коннектора к cassandra

  "org.postgresql" % "postgresql" % "42.6.0",

  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  "org.scalatest" %% "scalatest" % "3.2.16" % Test  // Test - зависимость будет присутствовать только в test
)