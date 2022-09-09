package l_5

import l_5.DataFrame_5.printPhysicalPlan
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object DataFrame_6 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_5")
    .getOrCreate

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._
  println(sc.uiWebUrl)

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame = spark
    .read
    .options(csvOptions)
    .csv("src/main/resources/l_3/airport-codes.csv")

  /** Оптимизация соединений и группировок. */
  val left: DataFrame = airportsDf.select('type, 'ident, 'iso_country)

  val right: DataFrame = airportsDf
    .groupBy('type)
    .count

  /** BroadcastHash Join. */
  import org.apache.spark.sql.functions.broadcast
  val resDf: DataFrame = left.join(broadcast(right), Seq("type"), "inner")

  printPhysicalPlan(resDf)
  /*
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [type#17, ident#16, iso_country#21, count#56L]
       +- BroadcastHashJoin [type#17], [type#60], Inner, BuildRight, false
          :- Filter isnotnull(type#17)
          :  +- FileScan csv [ident#16,type#17,iso_country#21] Batched: false, DataFilters: [isnotnull(type#17)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [IsNotNull(type)], ReadSchema: struct<ident:string,type:string,iso_country:string>
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [id=#57]
             +- HashAggregate(keys=[type#60], functions=[count(1)], output=[type#60, count#56L])
                +- Exchange hashpartitioning(type#60, 200), ENSURE_REQUIREMENTS, [id=#54]
                   +- HashAggregate(keys=[type#60], functions=[partial_count(1)], output=[type#60, count#76L])
                      +- Filter isnotnull(type#60)
                         +- FileScan csv [type#60] Batched: false, DataFilters: [isnotnull(type#60)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [IsNotNull(type)], ReadSchema: struct<type:string>
   */

  val left2: DataFrame = airportsDf
    .select('type, 'ident, 'iso_country)
    .localCheckpoint

  val right2: DataFrame = airportsDf
    .groupBy('type)
    .count
    .localCheckpoint

  val resDf2: DataFrame = left2.join(broadcast(right2), Seq("type"), "inner")

  printPhysicalPlan(resDf2)
  /** PO BroadcastExchange. */
  /*
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [type#17, ident#16, iso_country#21, count#108L]
       +- BroadcastHashJoin [type#17], [type#107], Inner, BuildRight, false
          :- Filter isnotnull(type#17)
          :  +- Scan ExistingRDD[type#17,ident#16,iso_country#21]
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [id=#128]
             // null c null не джойнятся
             +- Filter isnotnull(type#107)
                +- Scan ExistingRDD[type#107,count#108L]
   */

  left2.printSchema
  right2.printSchema

  /** Примеры equ-join. */
  left2.join(right2, "type" :: Nil, "inner")
  left2.as("left").join(right2.as("right"), expr(""" left.iso_country = right.type """))

  /** Примеры non-equ join. */
  left2.as("left").join(right2.as("right"), expr(""" left.iso_country < right.type """))
  left2.as("left").join(right2.as("right"), expr(""" (left.iso_country < right.type) == true """))


  /**
   * Автоматическое вычисление объема данных в датафрейме - часто работает некорректно.
   * Лучше отключать.
   */
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

  /** SortMerge Join. */
  val resDf3: DataFrame = left2.join(right2, Seq("type"), "inner")
  printPhysicalPlan(resDf3)
  /*
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [type#17, ident#16, iso_country#21, count#156L]
       +- SortMergeJoin [type#17], [type#155], Inner
          :- Sort [type#17 ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(type#17, 200), ENSURE_REQUIREMENTS, [id=#156]
          :     +- Filter isnotnull(type#17)
          :        +- Scan ExistingRDD[type#17,ident#16,iso_country#21]
          +- Sort [type#155 ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(type#155, 200), ENSURE_REQUIREMENTS, [id=#157]
                +- Filter isnotnull(type#155)
                   +- Scan ExistingRDD[type#155,count#156L]
   */

  /**
   * Проблема джоина с перекошенными партициями - решается разделением датасета на более мелкие и последующим юнионом.
   * Вариант с партицированными данными.
   */
  /*
    leftDf - partitionBy - y/m/d
    rightDf - partitionBy - y/m/d

    val dates = List(???)

    dates
      .map { date =>
        leftDf.filter(dateExpression).join(right.filter(dateExpression), Seq(???), "inner")
      }
      .reduce((acc, df) => acc.unionAll(df))

    !!! unionAll - важен порядок колонок.
   */


  import org.apache.spark.sql.functions.{udf, col}

  /** BroadcastNestedLoop Join. */
  /**
   * Не смотря на то, что UDF сравнивает два ключа (т.е. фактически equ join), Spark ничего не знает про UDF
   * и не может применить BroadcastHashJoin/SortMergeJoin.
   *
   * Через udf можно сделать null-safe join.
   */
  val compareUdf: UserDefinedFunction = udf { (leftVal: String, rightVal: String) => leftVal == rightVal }

  val joinExpr: Column = compareUdf(col("left.type"), col("right.type"))

  val resDf4: DataFrame = left2.as("left").join(broadcast(right2).as("right"), joinExpr, "inner")
  printPhysicalPlan(resDf4)
  /*
    // Нет ни срезов ни проекций т.к. используется udf
    AdaptiveSparkPlan isFinalPlan=false
    +- BroadcastNestedLoopJoin BuildRight, Inner, UDF(type#17, type#164)
       :- Scan ExistingRDD[type#17,ident#16,iso_country#21]
       +- BroadcastExchange IdentityBroadcastMode, [id=#174]
          // нет фильтров isnotnull
          +- Scan ExistingRDD[type#164,count#165L]
   */


  val right3: DataFrame = airportsDf
    .groupBy('type)
    .count()
    .localCheckpoint()

  val resDf5: DataFrame = left2.as("left").join(right.as("right"), joinExpr, "inner")
//  val resDf5: DataFrame = left2.as("left").join(right3.as("right"), joinExpr, "inner")
  printPhysicalPlan(resDf5)
  /*
    AdaptiveSparkPlan isFinalPlan=false
    +- CartesianProduct UDF(type#17, type#189)
       :- Scan ExistingRDD[type#17,ident#16,iso_country#21]
       +- HashAggregate(keys=[type#189], functions=[count(1)], output=[type#189, count#56L])
          +- Exchange hashpartitioning(type#189, 200), ENSURE_REQUIREMENTS, [id=#195]
             +- HashAggregate(keys=[type#189], functions=[partial_count(1)], output=[type#189, count#76L])
                +- FileScan csv [type#189] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<type:string>
   */

  println(
    s"""Partition summary:
       |left2=${left2.rdd.getNumPartitions}
       |right3=${right3.rdd.getNumPartitions}
       |resDf5=${resDf5.rdd.getNumPartitions}
       |""".stripMargin)


  /**
   * Снижение объема shuffle.
   *
   * Предварительное репартиционирование по ключам имеет смысл делать если после него идет
   * несколько операций требующих репартицирования по одинаковым ключам (Exchange hashpartitioning).
   */
  spark.time {
    val leftDf: DataFrame = airportsDf

    val rightDf: DataFrame = airportsDf
      .groupBy('type)
      .count

    val joined: DataFrame = leftDf.join(rightDf, Seq("type"))
    joined.count
  }  // 651 ms

  spark.time {
    /**
     * Экономия - Spark UI SQL:
     * 1 Scan csv - т.к. результат репартицирования - файлы на файловой системе воркеров (~кэш) - второе чтение из источника - не нужно.
     * 1 Exchange hashpartitioning.
     */
    val airportsRepDf: Dataset[Row] = airportsDf.repartition(200, col("type"))  // 1008 ms
//    val airportsRepDf: Dataset[Row] = airportsDf.repartition(10, col("type"))  // 454 ms

    val leftDf: Dataset[Row] = airportsRepDf

    val rightDf: DataFrame = airportsRepDf
      .groupBy('type)
      .count

    val joined: DataFrame = leftDf.join(rightDf, Seq("type"))
//    printPhysicalPlan(joined)
    joined.count
  }

  Thread.sleep(1000000)
}
