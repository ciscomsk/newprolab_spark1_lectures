package l_11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count, lit, max, struct, to_json, udf}

object AntiPatterns extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("l_11")
      .master("local[*]")
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


  /** 1. ExchangeSingle partition */
  val emptyWindow: WindowSpec = Window.partitionBy()

  val rankedDf: DataFrame =
    airportsDf
      .select(
        $"ident",
        $"iso_country",
        $"elevation_ft",
        $"type",
        count("*").over(emptyWindow)
      )

//  rankedDf.show(20, truncate = false)
//  println()
//  rankedDf.explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Window [count(1) windowspecdefinition(specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS count(1) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)#43L]
       // !!! Exchange SinglePartition
       +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=72]
          +- Project [ident#17, iso_country#22, elevation_ft#20, type#18]
             +- FileScan csv [ident#17,type#18,elevation_ft#20,iso_country#22] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,elevation_ft:int,iso_country:string>
   */
  println()


  /**
   * 2.
   * в Spark 2.x
   * repartition(1).orderBy - вызовет репартицирование (Exchange rangepartitioning) в 200 партиций
   *
   * решение - sortWithinPartitions
   *
   * В Spark 3.5.1
   * repartition(1).orderBy не вызывает репартицирования - т.к. AQE по умолчанию включен (spark.sql.adaptive.enabled = true)
   */

//  spark.conf.set("spark.sql.adaptive.enabled", "false")

  val repartitionOrderByDf: Dataset[Row] =
    airportsDf
      .groupBy($"iso_country")
      .agg(max($"elevation_ft").alias("height"))
      .repartition(1)
//      .repartition(2)
      /** если значения в row группах паркета хранятся отсортированными - меньше объем + быстрее чтение */
      .orderBy($"height".asc)
//      .sortWithinPartitions($"height".asc)

  repartitionOrderByDf
    .write
    .mode(SaveMode.Ignore)
//    .parquet("src/main/resources/l_11/anti2.parquet_orderBy_repartition_1")
//    .parquet("src/main/resources/l_11/anti2.parquet_orderBy_repartition_2")
//    .parquet("src/main/resources/l_11/anti2.parquet_sortWithinPartitions_repartition_1")
//    .parquet("src/main/resources/l_11/anti2.parquet_sortWithinPartitions_repartition_2")

//  println(repartitionOrderByDf.rdd.getNumPartitions)
//  println()
//  repartitionOrderByDf.explain()

  // .repartition(1).orderBy($"height".asc)
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Sort [height#63 ASC NULLS FIRST], true, 0
       +- Exchange SinglePartition, REPARTITION_BY_NUM, [plan_id=237]
          +- HashAggregate(keys=[iso_country#22], functions=[max(elevation_ft#20)])
             +- Exchange hashpartitioning(iso_country#22, 200), ENSURE_REQUIREMENTS, [plan_id=235]
                +- HashAggregate(keys=[iso_country#22], functions=[partial_max(elevation_ft#20)])
                   +- FileScan csv [elevation_ft#20,iso_country#22] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<elevation_ft:int,iso_country:string>
   */

  // .repartition(2).orderBy($"height".asc)
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Sort [height#63 ASC NULLS FIRST], true, 0
       +- Exchange rangepartitioning(height#63 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=304]
          +- Exchange RoundRobinPartitioning(2), REPARTITION_BY_NUM, [plan_id=302]
             +- HashAggregate(keys=[iso_country#22], functions=[max(elevation_ft#20)])
                +- Exchange hashpartitioning(iso_country#22, 200), ENSURE_REQUIREMENTS, [plan_id=300]
                   +- HashAggregate(keys=[iso_country#22], functions=[partial_max(elevation_ft#20)])
                      +- FileScan csv [elevation_ft#20,iso_country#22] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<elevation_ft:int,iso_country:string>
   */

  // .repartition(1).sortWithinPartitions($"height".asc)
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Sort [height#63 ASC NULLS FIRST], false, 0
       +- Exchange SinglePartition, REPARTITION_BY_NUM, [plan_id=237]
          +- HashAggregate(keys=[iso_country#22], functions=[max(elevation_ft#20)])
             +- Exchange hashpartitioning(iso_country#22, 200), ENSURE_REQUIREMENTS, [plan_id=235]
                +- HashAggregate(keys=[iso_country#22], functions=[partial_max(elevation_ft#20)])
                   +- FileScan csv [elevation_ft#20,iso_country#22] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<elevation_ft:int,iso_country:string>
   */

  // .repartition(2).sortWithinPartitions($"height".asc)
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Sort [height#89 ASC NULLS FIRST], false, 0
       +- Exchange RoundRobinPartitioning(2), REPARTITION_BY_NUM, [plan_id=292]
          +- HashAggregate(keys=[iso_country#22], functions=[max(elevation_ft#20)])
             +- Exchange hashpartitioning(iso_country#22, 200), ENSURE_REQUIREMENTS, [plan_id=290]
                +- HashAggregate(keys=[iso_country#22], functions=[partial_max(elevation_ft#20)])
                   +- FileScan csv [elevation_ft#20,iso_country#22] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<elevation_ft:int,iso_country:string>
   */


  /** 3. Dataset API */
  val jsonedDs: Dataset[String] = airportsDf.toJSON

//  jsonedDs.show(20, truncate = false)
//  jsonedDs.explain()
  /**
   * !!! 3 физических оператора
   * 1. DeserializeToObject - InternalRow -> Java object
   * 2. MapPartitions - применение функции над Java object
   * 3. SerializeFromObject - Java object -> InternalRow
   */
  /*
    == Physical Plan ==
    *(1) SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false, true) AS value#71]
    +- MapPartitions org.apache.spark.sql.Dataset$$Lambda$2932/0x00000008012eb840@281a54b, obj#70: java.lang.String
       +- DeserializeToObject createexternalrow(ident#17.toString, type#18.toString, name#19.toString, staticinvoke(class java.lang.Integer, ObjectType(class java.lang.Integer), valueOf, elevation_ft#20, true, false, true), continent#21.toString, iso_country#22.toString, iso_region#23.toString, municipality#24.toString, gps_code#25.toString, iata_code#26.toString, local_code#27.toString, coordinates#28.toString, StructField(ident,StringType,true), StructField(type,StringType,true), StructField(name,StringType,true), StructField(elevation_ft,IntegerType,true), StructField(continent,StringType,true), StructField(iso_country,StringType,true), StructField(iso_region,StringType,true), StructField(municipality,StringType,true), StructField(gps_code,StringType,true), StructField(iata_code,StringType,true), StructField(local_code,StringType,true), StructField(coordinates,StringType,true)), obj#69: org.apache.spark.sql.Row
          +- FileScan csv [ident#17,type#18,name#19,elevation_ft#20,continent#21,iso_country#22,iso_region#23,municipality#24,gps_code#25,iata_code#26,local_code#27,coordinates#28] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
   */

  val jsonedDf: DataFrame = airportsDf.select(to_json(struct(col("*"))))
//  jsonedDf.explain()
  /*
    == Physical Plan ==
    Project [to_json(struct(ident, ident#17, type, type#18, name, name#19, elevation_ft, elevation_ft#20, continent, continent#21, iso_country, iso_country#22, iso_region, iso_region#23, municipality, municipality#24, gps_code, gps_code#25, iata_code, iata_code#26, local_code, local_code#27, coordinates, coordinates#28), Some(Europe/Moscow)) AS to_json(struct(ident, type, name, elevation_ft, continent, iso_country, iso_region, municipality, gps_code, iata_code, local_code, coordinates))#89]
    +- FileScan csv [ident#17,type#18,name#19,elevation_ft#20,continent#21,iso_country#22,iso_region#23,municipality#24,gps_code#25,iata_code#26,local_code#27,coordinates#28] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
   */

  case class Apple(size: Int, color: String)

  val testDs: Dataset[Int] =
    List(Apple(1, "red"))
      .toDS()
      .map(_.size)

//  testDs.show()
//  testDs.explain()
  /*
    == Physical Plan ==
    *(1) SerializeFromObject [input[0, int, false] AS value#101]
    +- *(1) MapElements l_11.AntiPatterns$$$Lambda$3181/0x00000008013b9840@7e113065, obj#100: int
       +- *(1) DeserializeToObject newInstance(class l_11.AntiPatterns$Apple), obj#99: l_11.AntiPatterns$Apple
          +- *(1) LocalTableScan [size#93, color#94]
   */


  /** 4. UDF */
  /** 4.1 Scala types */
  val mega_udf_scalaType: UserDefinedFunction = udf { (left: Int, right: String) => "ok" }

  spark
    .range(1)
    .select(
      lit(1).as("left"),
      lit("foo").alias("right")
    )
    .select(mega_udf_scalaType($"left", $"right"))
//    .show()
  /*
    +----------------+
    |UDF(left, right)|
    +----------------+
    |              ok|
    +----------------+
   */

  /** !!! Int - скаловый тип -> не может быть null */
  // err - an expression of type Null is ineligible for implicit conversion
//  val scalaInt: Int = null
//  println(null.asInstanceOf[Int]) // = 0

  /** java.lang.Integer -> scala.Int */
//  println(null.asInstanceOf[java.lang.Integer].toInt) // = 0

  spark
    .range(1)
    .select(
      lit(null).as("left"),
      lit("foo").alias("right")
    )
    .select(mega_udf_scalaType($"left", $"right"))
//    .show()
  /*
    +----------------+
    |UDF(left, right)|
    +----------------+
    |            NULL|
    +----------------+
   */

  /** String может быть null */
  val javaString: java.lang.String = null
//  println(javaString == null) // = true

  spark
    .range(1)
    .select(
      lit(1).as("left"),
      lit(null).alias("right")
    )
    .select(mega_udf_scalaType($"left", $"right"))
//    .show()
  /*
    +----------------+
    |UDF(left, right)|
    +----------------+
    |              ok|
    +----------------+
   */

  /** решение - использовать java типы */
  val mega_udf_javaType: UserDefinedFunction = udf { (left: java.lang.Integer, right: String) => "ok" }

  spark
    .range(1)
    .select(
      lit(null).as("left"),
      lit("foo").alias("right")
    )
    .select(mega_udf_javaType($"left", $"right"))
//    .show()
  /*
    +----------------+
    |UDF(left, right)|
    +----------------+
    |              ok|
    +----------------+
   */

  /** 4.2 Применение udf к каждой колонке */
  case class Foo(first: Int, second: Int, third: Int)

  val mega_udf2: UserDefinedFunction = udf { () => Thread.sleep(1000); Foo(1, 2, 3) }

  val udfDf: DataFrame =
    spark
      .range(0, 10, 1, 1)
      .select(mega_udf2().as("res"))

//  udfDf.printSchema()
  /*
    root
     |-- res: struct (nullable = true)
     |    |-- first: integer (nullable = false)
     |    |-- second: integer (nullable = false)
     |    |-- third: integer (nullable = false)
   */
//  udfDf.show()

  spark.time {
    val testDf1: DataFrame =
      spark
        .range(0, 10, 1, 1)
        .select(mega_udf2().as("res"))
        .select($"res.first", $"res.second", $"res.third")

//    testDf1.explain(true)
    /*
      == Parsed Logical Plan ==
      'Project ['res.first, 'res.second, 'res.third]
      +- Project [UDF() AS res#163]
         +- Range (0, 10, step=1, splits=Some(1))

      == Analyzed Logical Plan ==
      first: int, second: int, third: int
      Project [res#163.first AS first#165, res#163.second AS second#166, res#163.third AS third#167]
      +- Project [UDF() AS res#163]
         +- Range (0, 10, step=1, splits=Some(1))

      == Optimized Logical Plan ==
      Project [res#163.first AS first#165, res#163.second AS second#166, res#163.third AS third#167]
      +- Project [UDF() AS res#163]
         +- Range (0, 10, step=1, splits=Some(1))

      == Physical Plan ==
      *(1) Project [res#163.first AS first#165, res#163.second AS second#166, res#163.third AS third#167]
      +- *(1) Project [UDF() AS res#163]
         +- *(1) Range (0, 10, step=1, splits=1)
     */

//    testDf1.show()
  }  // 10276 ms
  /** В Spark 2.х ~30 c */
  println()

  spark.time {
    val testDf2: DataFrame =
      spark
        .range(0, 10, 1, 1)
        .select(mega_udf2().as("res"))
        .select($"res.*")

//    testDf2.explain(true)
    /*
      == Parsed Logical Plan ==
      'Project [res.*]
      +- Project [UDF() AS res#168]
         +- Range (0, 10, step=1, splits=Some(1))

      == Analyzed Logical Plan ==
      first: int, second: int, third: int
      Project [res#168.first AS first#170, res#168.second AS second#171, res#168.third AS third#172]
      +- Project [UDF() AS res#168]
         +- Range (0, 10, step=1, splits=Some(1))

      == Optimized Logical Plan ==
      Project [res#168.first AS first#170, res#168.second AS second#171, res#168.third AS third#172]
      +- Project [UDF() AS res#168]
         +- Range (0, 10, step=1, splits=Some(1))

      == Physical Plan ==
      *(1) Project [res#168.first AS first#170, res#168.second AS second#171, res#168.third AS third#172]
      +- *(1) Project [UDF() AS res#168]
         +- *(1) Range (0, 10, step=1, splits=1)
     */

//    testDf2.show()
  }  // 10271 ms
  /** В Spark 2.х ~30 c */
  println()

  /** решение - asNondeterministic */
  val mega_udf2_nd: UserDefinedFunction = udf { () => Thread.sleep(1000); Foo(1, 2, 3) }.asNondeterministic()

  spark.time {
    val testDf3: DataFrame =
      spark
        .range(0, 10, 1, 1)
        .select(mega_udf2_nd().as("res"))
        .select($"res.first", $"res.second", $"res.third")

//    testDf3.explain(true)
    /*
      == Parsed Logical Plan ==
      'Project ['res.first, 'res.second, 'res.third]
      +- Project [UDF() AS res#181]
         +- Range (0, 10, step=1, splits=Some(1))

      == Analyzed Logical Plan ==
      first: int, second: int, third: int
      Project [res#181.first AS first#183, res#181.second AS second#184, res#181.third AS third#185]
      +- Project [UDF() AS res#181]
         +- Range (0, 10, step=1, splits=Some(1))

      == Optimized Logical Plan ==
      Project [res#181.first AS first#183, res#181.second AS second#184, res#181.third AS third#185]
      +- Project [UDF() AS res#181]
         +- Range (0, 10, step=1, splits=Some(1))

      == Physical Plan ==
      *(1) Project [res#181.first AS first#183, res#181.second AS second#184, res#181.third AS third#185]
      +- *(1) Project [UDF() AS res#181]
         +- *(1) Range (0, 10, step=1, splits=1)
     */

//    testDf3.show()
  }  // 10297 ms
  /** В Spark 2.х ~10 c */
  println()


  /** 5. Coalesce */
  spark.time {
    val testDf4: DataFrame =
      spark
        .range(0, 10, 1, 2)
        .withColumn("foo", mega_udf2())
        .coalesce(1)
    /**
     * фактически (несмотря на правильный физический план) превращается в
     *
     * spark.
     *   .range(0, 10, 1, 2)
     *   .coalesce(1)
     *   .withColumn("foo", mega_udf2())
     */

//    testDf4.explain()
    /*
      == Physical Plan ==
      Coalesce 1
      +- *(1) Project [id#189L, UDF() AS foo#191]
         +- *(1) Range (0, 10, step=1, splits=2)
     */

//    testDf4.collect()
  }  // !!! 10281 ms, а не 5с как должно быть
  println()

  /** решение - cache().count() */
  spark.time {
    val testDf5: DataFrame =
      spark
        .range(0, 10, 1, 2)
        .withColumn("foo", mega_udf2())

    testDf5.cache()
//    testDf5.count()

//    testDf5.coalesce(1).explain()
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Coalesce 1
         +- InMemoryTableScan [id#194L, foo#196]
               +- InMemoryRelation [id#194L, foo#196], StorageLevel(disk, memory, deserialized, 1 replicas)
                     +- *(1) Project [id#194L, UDF() AS foo#196]
                        +- *(1) Range (0, 10, step=1, splits=2)
     */

//    testDf5.coalesce(1).collect()
  }  // 5559 ms
  println()


  /**
   * 6. Partial caching
   * !!! не воспроизводится в Spark 3.5.1 - в кэше будет 2 партиции
   */
  import sys.process._
  "cp -f src/main/resources/l_11/source/1.txt src/main/resources/l_11/cache/1.txt".!
  "cp -f src/main/resources/l_11/source/2.txt src/main/resources/l_11/cache/2.txt".!

  val textDf: DataFrame =
    spark
      .read
      .text("src/main/resources/l_11/cache")

  println(s"textDf.rdd.getNumPartitions: ${textDf.rdd.getNumPartitions}")
//  textDf.show()

  textDf.cache()
  textDf.show(1)

  "cp -f src/main/resources/l_11/source/a.txt src/main/resources/l_11/cache/1.txt".!
  "cp -f src/main/resources/l_11/source/b.txt src/main/resources/l_11/cache/2.txt".!

  textDf.show()

//  spark
//    .sharedState
//    .cacheManager
//    .clearCache()

  println(sc.uiWebUrl)
  Thread.sleep(1_000_000)

  spark.stop()
}
