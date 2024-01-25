name := "lectures"

version := "0.1"

scalaVersion := "2.13.12"
val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion, // % Provided,  // Provided - зависимость не будет включена при сборке fat jar (плагин assembly)
//  "org.apache.spark" %% "spark-repl" % "sparkVersion" % Provided,  // для 11 лекции
  "org.apache.spark" %% "spark-hadoop-cloud" % sparkVersion,

  "org.elasticsearch" %% "elasticsearch-spark-30" % "8.11.0",

  "com.datastax.spark" %% "spark-cassandra-connector" % "3.4.1",
//  "joda-time" % "joda-time" % "2.12.5",  // для коннектора к cassandra

  "org.postgresql" % "postgresql" % "42.6.0",

  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

//  "org.bouncycastle" % "bcpkix-jdk18on" % "1.76",

  "org.scalatest" %% "scalatest" % "3.2.17" % Test  // Test - зависимость будет присутствовать только в test
)