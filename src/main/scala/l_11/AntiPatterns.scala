package l_11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count, lit, max, struct, to_json, udf}

object AntiPatterns extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("l_11")
      .master("local[*]")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  val sparkUiUrl: Option[String] = sc.uiWebUrl
  println(sparkUiUrl)


  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")


  /** 1. ExchangeSingle partition. */
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

  rankedDf.show(20, truncate = false)
  println()

  rankedDf.explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Window [count(1) windowspecdefinition(specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS count(1) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)#42L]
       // Exchange SinglePartition
       +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#72]
          +- Project [ident#16, iso_country#21, elevation_ft#19, type#17]
             +- FileScan csv [ident#16,type#17,elevation_ft#19,iso_country#21] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,elevation_ft:int,iso_country:string>
   */


  /**
   * 2. repartition(1).orderBy - orderBy вызовет репартицирование в 200 (по-умолчанию) партиций - в Spark 2.x.
   * Решение в Spark 2.x - замена orderBy на sortWithinPartitions.
   *
   * В Spark 3.2.0 - orderBy не вызывает репартицирования.
   */
  val repartitionOrderByDf: Dataset[Row] =
    airportsDf
      .groupBy($"iso_country")
      .agg(max($"elevation_ft").alias("height"))
      .repartition(1)
      /** Если значения в row группах паркета отсортированы - меньше объем + быстрее чтение. */
      .orderBy($"height".asc)

  repartitionOrderByDf
    .write
    .mode(SaveMode.Ignore)
    .parquet("src/main/resources/l_11/anti2.parquet")

  println(repartitionOrderByDf.rdd.getNumPartitions)
  repartitionOrderByDf.explain()
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Sort [height#87 ASC NULLS FIRST], true, 0
       +- Exchange SinglePartition, REPARTITION_BY_NUM, [id=#268]
          +- HashAggregate(keys=[iso_country#21], functions=[max(elevation_ft#19)])
             +- Exchange hashpartitioning(iso_country#21, 200), ENSURE_REQUIREMENTS, [id=#266]
                +- HashAggregate(keys=[iso_country#21], functions=[partial_max(elevation_ft#19)])
                   +- FileScan csv [elevation_ft#19,iso_country#21] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<elevation_ft:int,iso_country:string>
   */


  /** 3. Dataset API. */
  val jsonedDs: Dataset[String] = airportsDf.toJSON

  jsonedDs.show(20)
  jsonedDs.explain()
  /**
   * !!! 3 физических оператора.
   * 1. DeserializeToObject - InternalRow => Java object.
   * 2. MapPartitions - применение функции над Java object.
   * 3. SerializeFromObject - Java object => InternalRow.
   */
  /*
    == Physical Plan ==
    *(1) SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false) AS value#104]
    +- MapPartitions org.apache.spark.sql.Dataset$$Lambda$3289/0x0000000801314040@2c1b10f2, obj#103: java.lang.String
       +- DeserializeToObject createexternalrow(ident#16.toString, type#17.toString, name#18.toString, elevation_ft#19, continent#20.toString, iso_country#21.toString, iso_region#22.toString, municipality#23.toString, gps_code#24.toString, iata_code#25.toString, local_code#26.toString, coordinates#27.toString, StructField(ident,StringType,true), StructField(type,StringType,true), StructField(name,StringType,true), StructField(elevation_ft,IntegerType,true), StructField(continent,StringType,true), StructField(iso_country,StringType,true), StructField(iso_region,StringType,true), StructField(municipality,StringType,true), StructField(gps_code,StringType,true), StructField(iata_code,StringType,true), StructField(local_code,StringType,true), StructField(coordinates,StringType,true)), obj#102: org.apache.spark.sql.Row
          +- FileScan csv [ident#16,type#17,name#18,elevation_ft#19,continent#20,iso_country#21,iso_region#22,municipality#23,gps_code#24,iata_code#25,local_code#26,coordinates#27] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
   */

  val jsonedDf: DataFrame = airportsDf.select(to_json(struct(col("*"))))
  jsonedDf.explain()
  /*
    == Physical Plan ==
    Project [to_json(struct(ident, ident#16, type, type#17, name, name#18, elevation_ft, elevation_ft#19, continent, continent#20, iso_country, iso_country#21, iso_region, iso_region#22, municipality, municipality#23, gps_code, gps_code#24, iata_code, iata_code#25, local_code, local_code#26, coordinates, coordinates#27), Some(Europe/Moscow)) AS to_json(struct(ident, type, name, elevation_ft, continent, iso_country, iso_region, municipality, gps_code, iata_code, local_code, coordinates))#122]
    +- FileScan csv [ident#16,type#17,name#18,elevation_ft#19,continent#20,iso_country#21,iso_region#22,municipality#23,gps_code#24,iata_code#25,local_code#26,coordinates#27] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/repos/newprolab/spark_1/lectures/src/main/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
   */

  case class Apple(size: Int, color: String)

  val testDs: Dataset[Int] =
    List(Apple(1, "red"))
      .toDS()
      .map(_.size)

  testDs.show()
  testDs.explain()
  /*
    == Physical Plan ==
    *(1) SerializeFromObject [input[0, int, false] AS value#132]
    +- *(1) MapElements l_11.AntiPatterns$$$Lambda$3368/0x000000080135d840@2257590f, obj#131: int
       +- *(1) DeserializeToObject newInstance(class l_11.AntiPatterns$Apple), obj#130: l_11.AntiPatterns$Apple
          +- *(1) LocalTableScan [size#124, color#125]
   */


  /** 4. UDF */
  /** 4.1 Scala types. */
  val mega_udf_scalaType: UserDefinedFunction = udf { (left: Int, right: String) => "ok" }

  spark
    .range(1)
    .select(
      lit(1).as("left"),
      lit("foo").alias("right")
    )
    .select(mega_udf_scalaType($"left", $"right"))
    .show()
  /*
    +----------------+
    |UDF(left, right)|
    +----------------+
    |              ok|
    +----------------+
   */

  /** Int - скаловый тип => не может быть null. */
  // err -  an expression of type Null is ineligible for implicit conversion
//  val scalaInt: Int = null

  spark
    .range(1)
    .select(
      lit(null).as("left"),
      lit("foo").alias("right")
    )
    .select(mega_udf_scalaType($"left", $"right"))
    .show()
  /*
    +----------------+
    |UDF(left, right)|
    +----------------+
    |            null|
    +----------------+
   */

  /**
   * Решение - использовать java типы.
   * Преобразование java.lang.Integer => Int - toInt (если null - будет исключение).
   */
  val mega_udf_javaType: UserDefinedFunction = udf { (left: java.lang.Integer, right: String) => "ok" }

  spark
    .range(1)
    .select(
      lit(null).as("left"),
      lit("foo").alias("right")
    )
    .select(mega_udf_javaType($"left", $"right"))
    .show()
  /*
    +----------------+
    |UDF(left, right)|
    +----------------+
    |              ok|
    +----------------+
   */

  /** String может быть null. */
  val javaString: java.lang.String = null
  println(javaString == null)

  spark
    .range(1)
    .select(
      lit(1).as("left"),
      lit(null).alias("right")
    )
    .select(mega_udf_scalaType($"left", $"right"))
    .show()
  /*
    +----------------+
    |UDF(left, right)|
    +----------------+
    |              ok|
    +----------------+
   */

  /** 4.2 Применение udf к каждой колонке. */
  case class Foo(first: Int, second: Int, third: Int)

  val mega_udf2: UserDefinedFunction = udf { () => Thread.sleep(1000); Foo(1, 2, 3) }

  val udfDf: DataFrame =
    spark
      .range(0, 10, 1, 1)
      .select(mega_udf2().as("res"))

  udfDf.printSchema()
//  udfDf.show()

  spark.time {
    val testDf1: DataFrame =
      spark
        .range(0, 10, 1, 1)
        .select(mega_udf2().as("res"))
        .select($"res.first", $"res.second", $"res.third")
        // == .select('res("first"), 'res("second"), 'res("third")

    testDf1.explain(true)
    /*
      == Parsed Logical Plan ==
      'Project ['res.first, 'res.second, 'res.third]
      +- Project [UDF() AS res#233]
         +- Range (0, 10, step=1, splits=Some(1))

      == Analyzed Logical Plan ==
      first: int, second: int, third: int
      Project [res#233.first AS first#235, res#233.second AS second#236, res#233.third AS third#237]
      +- Project [UDF() AS res#233]
         +- Range (0, 10, step=1, splits=Some(1))

      == Optimized Logical Plan ==
      Project [UDF().first AS first#235, UDF().second AS second#236, UDF().third AS third#237]
      +- Range (0, 10, step=1, splits=Some(1))

      == Physical Plan ==
      *(1) Project [UDF().first AS first#235, UDF().second AS second#236, UDF().third AS third#237]
      +- *(1) Range (0, 10, step=1, splits=1)
     */

//    testDf1.show()
  }  // 10120 ms
  /** В Spark 2.х ~ 30 c. */
  println()

  spark.time {
    val testDf2: DataFrame =
      spark
        .range(0, 10, 1, 1)
        .select(mega_udf2().as("res"))
        .select($"res.*")

    testDf2.explain(true)
    /*
      == Parsed Logical Plan ==
      'Project [List(res).*]
      +- Project [UDF() AS res#243]
         +- Range (0, 10, step=1, splits=Some(1))

      == Analyzed Logical Plan ==
      first: int, second: int, third: int
      Project [res#243.first AS first#245, res#243.second AS second#246, res#243.third AS third#247]
      +- Project [UDF() AS res#243]
         +- Range (0, 10, step=1, splits=Some(1))

      == Optimized Logical Plan ==
      Project [UDF().first AS first#245, UDF().second AS second#246, UDF().third AS third#247]
      +- Range (0, 10, step=1, splits=Some(1))

      == Physical Plan ==
      *(1) Project [UDF().first AS first#245, UDF().second AS second#246, UDF().third AS third#247]
      +- *(1) Range (0, 10, step=1, splits=1)
     */

//    testDf2.show()
  }  // 10077 ms
  /** В Spark 2.х ~ 30 c. */
  println()

  /** Решение - asNondeterministic. */
  val mega_udf2_nd: UserDefinedFunction = udf { () => Thread.sleep(1000); Foo(1, 2, 3) }.asNondeterministic()

  spark.time {
    val testDf3: DataFrame =
      spark
        .range(0, 10, 1, 1)
        .select(mega_udf2_nd().as("res"))
        .select($"res.first", $"res.second", $"res.third")

    testDf3.explain(true)
    /*
      == Parsed Logical Plan ==
      'Project ['res.first, 'res.second, 'res.third]
      +- Project [UDF() AS res#256]
         +- Range (0, 10, step=1, splits=Some(1))

      == Analyzed Logical Plan ==
      first: int, second: int, third: int
      Project [res#256.first AS first#258, res#256.second AS second#259, res#256.third AS third#260]
      +- Project [UDF() AS res#256]
         +- Range (0, 10, step=1, splits=Some(1))

      == Optimized Logical Plan ==
      Project [res#256.first AS first#258, res#256.second AS second#259, res#256.third AS third#260]
      +- Project [UDF() AS res#256]
         +- Range (0, 10, step=1, splits=Some(1))

      == Physical Plan ==
      *(1) Project [res#256.first AS first#258, res#256.second AS second#259, res#256.third AS third#260]
      // !!!
      +- *(1) Project [UDF() AS res#256]
         +- *(1) Range (0, 10, step=1, splits=1)
     */

//    testDf3.show()
  }  // 10120 ms
  /** В Spark 2.х ~ 10 c. */


  /** 5. Coalesce. */
  spark.time {
    val testDf4: DataFrame =
      spark
        .range(0, 10, 1, 2)
        .withColumn("foo", mega_udf2())
        .coalesce(1)
        /**
         * Фактически преобразовывается в (несмотря на правильный физический план):
         *
         * spark.
         *   .range(0, 10, 1, 2)
         *   .coalesce(1)
         *   .withColumn("foo", mega_udf2())
         */

    testDf4.explain()
    /*
      == Physical Plan ==
      Coalesce 1
      +- *(1) Project [id#264L, UDF() AS foo#266]
         +- *(1) Range (0, 10, step=1, splits=2)
     */
//    testDf4.collect()
  }  // 10122 ms, а не 5 с как должно быть
  println()

  /** Решение - cache().count() */
  spark.time {
    val testDf5: DataFrame =
      spark
        .range(0, 10, 1, 2)
        .withColumn("foo", mega_udf2())

    testDf5.cache()
    testDf5.count()

    testDf5.coalesce(1).explain()
    /*
      == Physical Plan ==
      Coalesce 1
      +- InMemoryTableScan [id#269L, foo#271]
            +- InMemoryRelation [id#269L, foo#271], StorageLevel(disk, memory, deserialized, 1 replicas)
                  +- *(1) Project [id#269L, UDF() AS foo#271]
                     +- *(1) Range (0, 10, step=1, splits=2)
     */
    testDf5.coalesce(1).collect()
  }  // 5342 ms


  /** 6. Partial caching. */
  val textDf: DataFrame =
    spark
      .read
      .text("src/main/resources/l_11/anti6.txt")

//  textDf.show()

  textDf.cache()
  textDf.show(1)

  /**
   * Программно меняем:
   * в 2.txt 2 => 1
   * в 1.txt 1 => 3
   */

  textDf.show()

  Thread.sleep(10000000)

  spark.sharedState.cacheManager.clearCache()
}
