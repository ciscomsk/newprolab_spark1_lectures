package l_5

import l_5.DataFrame_5.printPhysicalPlan
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object DataFrame_6 extends App {
  // не работает в Spark 3.3.0
//  Logger
//    .getLogger("org")
//    .setLevel(Level.OFF)

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._
  println(sc.uiWebUrl)

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
  import org.apache.spark.sql.functions.broadcast
  val resDf: DataFrame = leftDf.join(broadcast(rightDf), Seq("type"), "inner")

  printPhysicalPlan(resDf)
  /** Ниже более читаемый план с localCheckpoint */
  /*
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [type#18, ident#17, iso_country#22, count#58L]
       +- BroadcastHashJoin [type#18], [type#62], Inner, BuildRight, false
          // leftDf
          :- Filter isnotnull(type#18)
          :  +- FileScan csv [ident#17,type#18,iso_country#22] Batched: false, DataFilters: [isnotnull(type#18)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn_2/courses/spark/newprolab/spark_1/_repos/lectur..., PartitionFilters: [], PushedFilters: [IsNotNull(type)], ReadSchema: struct<ident:string,type:string,iso_country:string>
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [id=#57]
             // rightDf
             +- HashAggregate(keys=[type#62], functions=[count(1)], output=[type#62, count#58L])
                +- Exchange hashpartitioning(type#62, 200), ENSURE_REQUIREMENTS, [id=#54]
                   +- HashAggregate(keys=[type#62], functions=[partial_count(1)], output=[type#62, count#79L])
                      +- Filter isnotnull(type#62)
                         +- FileScan csv [type#62] Batched: false, DataFilters: [isnotnull(type#62)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn_2/courses/spark/newprolab/spark_1/_repos/lectur..., PartitionFilters: [], PushedFilters: [IsNotNull(type)], ReadSchema: struct<type:string>
   */

  val left2: DataFrame =
    airportsDf
      .select($"type", $"ident", $"iso_country")
      .localCheckpoint()

  val right2: DataFrame =
    airportsDf
      .groupBy($"type")
      .count()
      .localCheckpoint()

  val resDf2: DataFrame = left2.join(broadcast(right2), Seq("type"), "inner")

  printPhysicalPlan(resDf2)
  /** PO BroadcastHashJoin + BroadcastExchange_HashedRelationBroadcastMode */
  /*
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [type#18, ident#17, iso_country#22, count#111L]
       +- BroadcastHashJoin [type#18], [type#110], Inner, BuildRight, false
          :- Filter isnotnull(type#18)
          :  +- Scan ExistingRDD[type#18,ident#17,iso_country#22]
          // HashedRelationBroadcastMode
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [id=#128]
             // null c null не джойнятся
             +- Filter isnotnull(type#110)
                +- Scan ExistingRDD[type#110,count#111L]
   */

  left2.printSchema()
  right2.printSchema()

  /** Примеры equ-join */
  left2.join(right2, Seq("type"), "inner")
  left2.as("left").join(right2.as("right"), expr("left.iso_country = right.type"))

  /** Примеры non-equ join */
  left2.as("left").join(right2.as("right"), expr("left.iso_country < right.type"))
  // ==
  left2.as("left").join(right2.as("right"), expr("(left.iso_country < right.type) == true"))


  /** Автоматическое вычисление объема данных в датафрейме - часто работает некорректно => лучше отключить */
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

  /** SortMerge Join */
  val resDf3: DataFrame = left2.join(right2, Seq("type"), "inner")
  printPhysicalPlan(resDf3)
  /*
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [type#18, ident#17, iso_country#22, count#159L]
       +- SortMergeJoin [type#18], [type#158], Inner
          // Сортировка
          :- Sort [type#18 ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(type#18, 200), ENSURE_REQUIREMENTS, [id=#156]
          :     +- Filter isnotnull(type#18)
          :        +- Scan ExistingRDD[type#18,ident#17,iso_country#22]
          // Сортировка
          +- Sort [type#158 ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(type#158, 200), ENSURE_REQUIREMENTS, [id=#157]
                +- Filter isnotnull(type#158)
                   +- Scan ExistingRDD[type#158,count#159L]
   */

  /** Проблема джоина с перекошенными партициями - решается разделением датасета на более мелкие - их джоинами и последующим юнионом */
  /** Вариант с партицированными данными: */
  /*
    leftDf - partitionBy - y/m/d
    rightDf - partitionBy - y/m/d

    val dates = List(???)

    dates
      .map { date =>
        leftDf.filter(dateExpression).join(right.filter(dateExpression), Seq(???), "inner")
      }
      .reduce((acc, df) => acc.unionAll(df))

    !!! unionAll - важен порядок колонок, есть unionByName
   */


  import org.apache.spark.sql.functions.{udf, col}

  /** BroadcastNestedLoop Join */
  /**
   * Несмотря на то, что UDF сравнивает два ключа (т.е. фактически equ join), Spark ничего не знает про UDF
   * и не может применить BroadcastHashJoin/SortMergeJoin => применится BroadcastNestedLoopJoin /CartesianProduct
   *
   * Через udf можно сделать null-safe join
   */
  val compareUdf: UserDefinedFunction = udf { (leftVal: String, rightVal: String) => leftVal == rightVal }

  val joinExpr: Column = compareUdf(col("left.type"), col("right.type"))

  val resDf4: DataFrame = left2.as("left").join(broadcast(right2).as("right"), joinExpr, "inner")
  printPhysicalPlan(resDf4)
  /** PO BroadcastNestedLoopJoin + BroadcastExchange_IdentityBroadcastMode */
  /*
    // Нет ни срезов ни проекций т.к. используется udf
    AdaptiveSparkPlan isFinalPlan=false
    +- BroadcastNestedLoopJoin BuildRight, Inner, UDF(type#18, type#167)
       :- Scan ExistingRDD[type#18,ident#17,iso_country#22]
       // IdentityBroadcastMode
       +- BroadcastExchange IdentityBroadcastMode, [id=#174]
          // нет фильтра isnotnull
          +- Scan ExistingRDD[type#167,count#168L]
   */


  /** CartesianProduct */
  val resDf5: DataFrame = left2.as("left").join(right2.as("right"), joinExpr, "inner")
  printPhysicalPlan(resDf5)
  /*
    CartesianProduct UDF(type#18, type#212)
    :- *(1) Scan ExistingRDD[type#18,ident#17,iso_country#22]
    +- *(2) Scan ExistingRDD[type#212,count#213L]
   */

  println(
    s"""Partition summary:
       |left2=${left2.rdd.getNumPartitions}
       |right3=${right2.rdd.getNumPartitions}
       |resDf5=${resDf5.rdd.getNumPartitions}
       |""".stripMargin)
    // cartesian product num partitions == left num partitions * right num partitions

  /**
   * Снижение объема shuffle
   *
   * Предварительное репартиционирование по ключам имеет смысл делать если после него идет
   * несколько операций требующих репартицирования по одинаковым ключам (Exchange hashpartitioning)
   */
  spark.time {
    val leftDf: DataFrame = airportsDf

    val rightDf: DataFrame =
      airportsDf
        .groupBy($"type")
        .count()

    val joined: DataFrame = leftDf.join(rightDf, Seq("type"))

//    joined.explain()
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Project [type#18, ident#17, name#19, elevation_ft#20, continent#21, iso_country#22, iso_region#23, municipality#24, gps_code#25, iata_code#26, local_code#27, coordinates#28, count#231L]
         +- SortMergeJoin [type#18], [type#235], Inner
            :- Sort [type#18 ASC NULLS FIRST], false, 0
            :  +- Exchange hashpartitioning(type#18, 200), ENSURE_REQUIREMENTS, [id=#266]
            :     +- Filter isnotnull(type#18)
            :        +- FileScan csv [ident#17,type#18,name#19,elevation_ft#20,continent#21,iso_country#22,iso_region#23,municipality#24,gps_code#25,iata_code#26,local_code#27,coordinates#28] Batched: false, DataFilters: [isnotnull(type#18)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn_2/courses/spark/newprolab/spark_1/_repos/lectur..., PartitionFilters: [], PushedFilters: [IsNotNull(type)], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
            +- Sort [type#235 ASC NULLS FIRST], false, 0
               +- HashAggregate(keys=[type#235], functions=[count(1)])
                  +- Exchange hashpartitioning(type#235, 200), ENSURE_REQUIREMENTS, [id=#262]
                     +- HashAggregate(keys=[type#235], functions=[partial_count(1)])
                        +- Filter isnotnull(type#235)
                           +- FileScan csv [type#235] Batched: false, DataFilters: [isnotnull(type#235)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn_2/courses/spark/newprolab/spark_1/_repos/lectur..., PartitionFilters: [], PushedFilters: [IsNotNull(type)], ReadSchema: struct<type:string>
     */

    joined.count()
  }  // 626 ms

  spark.time {
    /**
     * Экономия - Spark UI => SQL/Dataframe:
     * -1 Scan csv - т.к. результат репартицирования это файлы на файловой системе воркеров (~cache DISK_ONLY) => второе чтение из источника не нужно
     * -1 Exchange hashpartitioning - т.к. репартицирование было по ключу join
     *
     * skipped stage == ранее был шаффл, который подходит для продолжения выполнения графа
     */
//    val airportsRepDf: Dataset[Row] = airportsDf.repartition(200, col("type"))  // 1008 ms
    val airportsRepDf: Dataset[Row] = airportsDf.repartition(10, col("type"))  // 456 ms

    val leftDf: Dataset[Row] = airportsRepDf

    val rightDf: DataFrame =
      airportsRepDf
        .groupBy($"type")
        .count()

    val joined: DataFrame = leftDf.join(rightDf, Seq("type"))

//    joined.explain()
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Project [type#18, ident#17, name#19, elevation_ft#20, continent#21, iso_country#22, iso_region#23, municipality#24, gps_code#25, iata_code#26, local_code#27, coordinates#28, count#295L]
         +- SortMergeJoin [type#18], [type#299], Inner
            :- Sort [type#18 ASC NULLS FIRST], false, 0
            :  +- Exchange hashpartitioning(type#18, 10), REPARTITION_BY_NUM, [id=#532]
            :     +- Filter isnotnull(type#18)
            :        +- FileScan csv [ident#17,type#18,name#19,elevation_ft#20,continent#21,iso_country#22,iso_region#23,municipality#24,gps_code#25,iata_code#26,local_code#27,coordinates#28] Batched: false, DataFilters: [isnotnull(type#18)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn_2/courses/spark/newprolab/spark_1/_repos/lectur..., PartitionFilters: [], PushedFilters: [IsNotNull(type)], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
            +- Sort [type#299 ASC NULLS FIRST], false, 0
               +- HashAggregate(keys=[type#299], functions=[count(1)])
                  +- HashAggregate(keys=[type#299], functions=[partial_count(1)])
                     +- Exchange hashpartitioning(type#299, 10), REPARTITION_BY_NUM, [id=#533]
                        +- Filter isnotnull(type#299)
                           +- FileScan csv [type#299] Batched: false, DataFilters: [isnotnull(type#299)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn_2/courses/spark/newprolab/spark_1/_repos/lectur..., PartitionFilters: [], PushedFilters: [IsNotNull(type)], ReadSchema: struct<type:string>
     */

    joined.count()
  }  // 1008/456 ms

  Thread.sleep(1000000)
}
