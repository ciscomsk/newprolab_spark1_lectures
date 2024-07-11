package l_5

import l_5.DataFrame_5.printPhysicalPlan
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{broadcast, expr, udf, col}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object DataFrame_6 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  import spark.implicits._

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

  /** Оптимизация соединений и группировок */
  val leftDf: DataFrame = airportsDf.select($"type", $"ident", $"iso_country")

  val rightDf: DataFrame =
    airportsDf
      .groupBy($"type")
      .count()

  /** BroadcastHash Join */
  val resDf: DataFrame = leftDf.join(broadcast(rightDf), Seq("type"), "inner")

  println()
  printPhysicalPlan(resDf)
  /*
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [type#18, ident#17, iso_country#22, count#58L]
       +- BroadcastHashJoin [type#18], [type#62], Inner, BuildRight, false
          // leftDf
          :- Filter isnotnull(type#18)
          :  +- FileScan csv [ident#17,type#18,iso_country#22] Batched: false, DataFilters: [isnotnull(type#18)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [IsNotNull(type)], ReadSchema: struct<ident:string,type:string,iso_country:string>
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=57]
             // rightDf
             +- HashAggregate(keys=[type#62], functions=[count(1)], output=[type#62, count#58L])
                +- Exchange hashpartitioning(type#62, 200), ENSURE_REQUIREMENTS, [plan_id=54]
                   +- HashAggregate(keys=[type#62], functions=[partial_count(1)], output=[type#62, count#79L])
                      +- Filter isnotnull(type#62)
                         +- FileScan csv [type#62] Batched: false, DataFilters: [isnotnull(type#62)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [IsNotNull(type)], ReadSchema: struct<type:string>
   */

  val leftDf2: DataFrame =
    airportsDf
      .select($"type", $"ident", $"iso_country")
      .localCheckpoint()

  val rightDf2: DataFrame =
    airportsDf
      .groupBy($"type")
      .count()
      .localCheckpoint()

  val resDf2: DataFrame = leftDf2.join(broadcast(rightDf2), Seq("type"), "inner")
  printPhysicalPlan(resDf2)

  /** BroadcastHash Join = PO BroadcastExchange HashedRelationBroadcastMode + PO BroadcastHashJoin */
  /** более читаемый план с localCheckpoint */
  /*
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [type#18, ident#17, iso_country#22, count#111L]
       +- BroadcastHashJoin [type#18], [type#110], Inner, BuildRight, false
          // null c null не джойнятся
          :- Filter isnotnull(type#18)
          :  +- Scan ExistingRDD[type#18,ident#17,iso_country#22]
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=128]
             // null c null не джойнятся
             +- Filter isnotnull(type#110)
                +- Scan ExistingRDD[type#110,count#111L]
   */

  leftDf2.printSchema()
  rightDf2.printSchema()

  /** примеры equ-join: */
  leftDf2.join(rightDf2, Seq("type"), "inner")
  leftDf2.as("left").join(rightDf2.as("right"), expr("left.iso_country = right.type"))

  /** примеры non-equ join: */
  leftDf2.as("left").join(rightDf2.as("right"), expr("left.iso_country < right.type"))
  // =
  leftDf2.as("left").join(rightDf2.as("right"), expr("(left.iso_country < right.type) = true"))

  /** автоматическое вычисление объема данных в датафрейме - часто работает некорректно => лучше отключить */
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")


  /** SortMerge Join */
  val resDf3: DataFrame = leftDf2.join(rightDf2, Seq("type"), "inner")
  printPhysicalPlan(resDf3)
  /*
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [type#18, ident#17, iso_country#22, count#159L]
       +- SortMergeJoin [type#18], [type#158], Inner
          // Сортировка
          :- Sort [type#18 ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(type#18, 200), ENSURE_REQUIREMENTS, [plan_id=156]
          :     +- Filter isnotnull(type#18)
          :        +- Scan ExistingRDD[type#18,ident#17,iso_country#22]
          // Сортировка
          +- Sort [type#158 ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(type#158, 200), ENSURE_REQUIREMENTS, [plan_id=157]
                +- Filter isnotnull(type#158)
                   +- Scan ExistingRDD[type#158,count#159L]
   */

  /** проблема джоина с перекошенными партициями - решается разделением датасета на более мелкие - их джоинами и последующим юнионом */
  /** вариант с партицированными данными: */
  /*
    leftDf - partitionBy y/m/d
    rightDf - partitionBy y/m/d

    val dates = List(...)

    dates
      .map { date =>
        leftDf.filter(date_condition).join(right.filter(date_condition), Seq(...), "inner")
      }
      .reduce((df1, df2) => df1.unionAll(df2))

    !!! unionAll - важен порядок колонок, есть unionByName
   */


  /** BroadcastNestedLoop Join */
  /**
   * несмотря на то, что udf проверяет на равенство два ключа (т.е. фактически equ-join), Spark ничего не знает про логику udf
   * и не может применить BroadcastHashJoin/SortMergeJoin -> применится BroadcastNestedLoopJoin/CartesianProduct
   *
   * через udf можно сделать null-safe join
   */
  val compareUdf: UserDefinedFunction = udf { (leftVal: String, rightVal: String) => leftVal == rightVal }
  val joinExpr: Column = compareUdf(col("left.type"), col("right.type"))

  val resDf4: DataFrame = leftDf2.as("left").join(broadcast(rightDf2).as("right"), joinExpr, "inner")
  printPhysicalPlan(resDf4)
  /** BroadcastNestedLoopJoin = PO BroadcastExchange IdentityBroadcastMode + PO BroadcastNestedLoopJoin */
  /*
    // нет ни срезов (isnotnull) ни проекций т.к. используется udf
    AdaptiveSparkPlan isFinalPlan=false
    +- BroadcastNestedLoopJoin BuildRight, Inner, UDF(type#18, type#167)
       :- Scan ExistingRDD[type#18,ident#17,iso_country#22]
       +- BroadcastExchange IdentityBroadcastMode, [plan_id=174]
          +- Scan ExistingRDD[type#167,count#168L]
   */


  /** CartesianProduct */
  val resDf5: DataFrame = leftDf2.as("left").join(rightDf2.as("right"), joinExpr, "inner")
  printPhysicalPlan(resDf5)
  /*
    CartesianProduct UDF(type#18, type#191)
    :- *(1) Scan ExistingRDD[type#18,ident#17,iso_country#22]
    +- *(2) Scan ExistingRDD[type#191,count#192L]
   */

  /** cartesian product result num partitions = left df num partitions * right df num partitions */
  println(
    s"""Partition summary:
       |left=${leftDf2.rdd.getNumPartitions}
       |right=${rightDf2.rdd.getNumPartitions}
       |result=${resDf5.rdd.getNumPartitions}
       |""".stripMargin
  )


  println(sc.uiWebUrl)
  Thread.sleep(1_000_000)

  spark.stop()
}
