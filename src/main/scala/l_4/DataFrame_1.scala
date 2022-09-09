package l_4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, explode, expr, length, lit, lower, split, struct, upper}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.lang

object DataFrame_1 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.OFF)

  val spark: SparkSession =
    SparkSession
      .builder
      .master("local[*]")
      .appName("DataFrame_1")
      .getOrCreate()

  val cityList: Vector[String] = Vector("Moscow", "Paris", "Madrid", "London", "New York")

  import spark.implicits._

  val df: DataFrame = cityList.toDF()
  df.printSchema()
  /*
    root
     |-- value: string (nullable = true)
   */

  /** Show работает аналогично take(rdd) - пытается взять данные из минимального количества партиций. */
  df.show
  df.show(numRows = 20, truncate = 100, vertical = true)


  /**
   * Алгоритм работы count(df):
   * 1. Рассчитывается количество элементов в каждой партиции.
   * 2. Агрегированные данные пересылаются в одну партицию (Exchange single partition) - где производится финальный reduce.
   * 3. Результат передается на драйвер.
   */
  println(df.count)
  println

  /** Filter == СРЕЗ. В отличии от RDD - принимает SQL выражение. */
  df
    // сигнатура: filter(condition: Column)
    .filter('value === "Moscow" ) // v1.1 // == value.===("Moscow")
    .show

  df
    /** $ - позволяет указывать колонки внутри структур. */
    .filter($"value" === "Moscow")  // v1.2
    .show

  val df2: Dataset[lang.Long] = spark.range(10)
  df2.show

  val df3: DataFrame = df2.select(struct(col("id").alias("id")).alias("foo"))
  df3.show
  df3.printSchema

  df3
    .select($"foo.id")
    .show

  df3
    .select($"foo.*")
    .show

  df3
    .toJSON
    .show


  df
    /** col - классический API без синтаксического сахара. */
    .filter(col("value") === "Moscow") // v1.3
    .show

  df
    // пишем DML в SQL-like формате
    .filter("value = 'Moscow'") // v2 - легко ошибиться и получить ошибку в рантайме
    .show

  df
    /** expr также используется для вызова SQL builtin функций, отсутствующих в org.apache.sql.functions */
    .filter(expr("value = 'Moscow'")) // v3 - промежуточный вариант между col и обычной строкой
    .show

  df
    .localCheckpoint
    .filter(expr("value = 'Moscow'"))
    .explain
  /** Каталист парсит выражение и создает план состоящий из физических операторов => план передается в тангстен для кодогенерации. */
  /*
    == Physical Plan ==
    *(1) Filter (isnotnull(value#1) AND (value#1 = Moscow))
    +- *(1) Scan ExistingRDD[value#1] // чтение существующего (находящегося в памяти) rdd
   */


  /** ПРОЕКЦИЯ == выборка существующих колонок или добавление новых. */
  /** withColumn - добавляет новую колонку. Является трансформацией и создает новый датафрейм (а не изменяет существующий). */
  df
    .withColumn("upperCity", upper('value))
    .show

  /** select - может быть использован не только для выбора определенных колонок, но и для создания новых. */
  val withUpperDf: DataFrame = df.select('value, upper('value).alias("upperCity"))
  withUpperDf.show

  /**
   * select(col(*)) - позволяет получить DF со всеми колонками - полезно, когда список всех колонок не известен,
   * и нужно выбрать все существующие + добавить новые колонки
   */
  withUpperDf
    .select(
      col("*"),
      /** alias == name == as */
      lower($"value").name("lowerCity"),
      (length('value) + 1).as("length"),
      lit("foo").alias("bar")
    )
    .show

  withUpperDf
    .localCheckpoint
    .select(
      col("*"),
      /** alias == name == as */
      lower($"value").name("lowerCity"),
      (length('value) + 1).as("length"),
      lit("foo").alias("bar")
    )
    .explain()
  /*
    == Physical Plan ==
    *(1) Project [value#1, upperCity#97, lower(value#1) AS lowerCity#140, (length(value#1) + 1) AS length#141, foo AS bar#142]
    +- *(1) Scan ExistingRDD[value#1,upperCity#97]
   */

  /** В select можно передать список колонок, используя обычные строки. */
  withUpperDf
    .select("value", "upperCity")
    .show

  /**
   * drop - удаляет колонки.
   * !!! drop не будет выдавать ошибку, если указана несуществующая колонка.
   */
  withUpperDf
    .drop("upperCity", "abraKadabra")
    .show

  withUpperDf
    .localCheckpoint
    .drop("upperCity", "abraKadabra")
    .explain
  /*
    == Physical Plan ==
    *(1) Project [value#1]
    +- *(1) Scan ExistingRDD[value#1,upperCity#97]
   */


  /** Conditions. */
  df
    .filter('value === "Moscow" or 'value === "Paris")
    .show

  df
    .filter('value.isin("Moscow", "Paris"))
    .show

  // and/between


  /** cache/persist/repartition - позволяют не вычислять граф несколько раз. */


  /** Data cleaning. */
  val testData: String =
    """{ "name": "Moscow", "country": "Rossiya", "continent": "Europe", "population": 12380664 }
      |{ "name": "Madrid", "country": "Spain" }
      |{ "name": "Paris", "country": "France", "continent": "Europe", "population": 2196936 }
      |{ "name": "Berlin", "country": "Germany", "continent": "Europe", "population": 3490105 }
      |{ "name": "Barselona", "country": "Spain", "continent": "Europe" }
      |{ "name": "Cairo", "country": "Egypt", "continent": "Africa", "population": 11922948 }
      |{ "name": "Cairo", "country": "Egypt", "continent": "Africa", "population": 11922948 }
      |{ "name": "New York", "country": "USA",
      |""".stripMargin

  val raw: DataFrame = spark
    .range(0, 1)
    .select(lit(testData).alias("value"))

  raw.show(truncate = false)

  val json: DataFrame = raw
    .select(split(col("value"), "\n").alias("value"))

  json.show(truncate = false)
  
  val jsonStrings: Column = split(col("value"), "\n").alias("value")

  val splitted: Dataset[String] = raw
    .select(explode(jsonStrings))
    .as[String]

  splitted.show(numRows = 10, truncate = false)

  /** !!! spark.read.json позволяет читать не только json-файлы, но и Dataset[String], содержащие JSON строки. */
  val df4: DataFrame = spark
    .read
    .json(splitted)

  df4.show(truncate = false)
  df4.printSchema


  val corruptData: Array[Row] = df4
    .select(col("_corrupt_record"))
    .na.drop("all")
    .collect

  println(corruptData.mkString("Array(", ", ", ")"))

  val fillData: Map[String, Any] = Map("continent" -> "Undefined", "population" -> 0)
  val replaceData: Map[String, String] = Map("Rossiya" -> "Russia")

  val cleanData: Dataset[Row] = df4
    .drop(col("_corrupt_record")) // drop - проекция
    /**
     * all - удаляются строки, где все колонки == null , any - удаляются строки, где хотя бы одна колонка == null.
     * Можно указать на какие колонки это будет распространяться.
     * */
    .na.drop("all") // na.drop - срез
    .na.fill(fillData)
    .na.replace("country", replaceData)
    /**
     * dropDuplicates без аргументов == distinct.
     * dropDuplicates неявно запускает шафл.
     */
    .dropDuplicates("continent", "country")

  cleanData
    .repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/l_4/cleandata")

  cleanData.show

  df4
    .drop(col("_corrupt_record"))
    .localCheckpoint
    .na.drop("all")
    .explain
  /*
    == Physical Plan ==
    *(1) Filter atleastnnonnulls(1, continent#207, country#208, name#209, population#210L)
    +- *(1) Scan ExistingRDD[continent#207,country#208,name#209,population#210L]
   */

  df4
    .drop(col("_corrupt_record"))
    .localCheckpoint
    .na.drop("all")
    .na.fill(fillData)
    .explain
  /*
    == Physical Plan ==
    *(1) Project [coalesce(continent#207, Undefined) AS continent#365, country#208, name#209, coalesce(population#210L, 0) AS population#366L]
    +- *(1) Filter atleastnnonnulls(1, continent#207, country#208, name#209, population#210L)
       +- *(1) Scan ExistingRDD[continent#207,country#208,name#209,population#210L]
   */

  df4
    .drop(col("_corrupt_record"))
    .localCheckpoint
    .na.drop("all")
    .na.fill(fillData)
    .na.replace("country", replaceData)
    .explain
  /*
    == Physical Plan ==
    *(1) Project [coalesce(continent#207, Undefined) AS continent#387, CASE WHEN (country#208 = Rossiya) THEN Russia ELSE country#208 END AS country#397, name#209, coalesce(population#210L, 0) AS population#388L]
    +- *(1) Filter atleastnnonnulls(1, continent#207, country#208, name#209, population#210L)
       +- *(1) Scan ExistingRDD[continent#207,country#208,name#209,population#210L]
   */

  df4
    .drop(col("_corrupt_record"))
    .localCheckpoint
    .na.drop("all")
    .na.fill(fillData)
    .na.replace("country", replaceData)

    .dropDuplicates("continent", "country")
    .explain

  /** Результат репартиционирования всегда находится на локальных дисках воркеров. */
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    // Удаление дубликатов внутри каждой партиции после репартиционирования.
    +- SortAggregate(key=[continent#418, country#428], functions=[first(name#209, false), first(population#419L, false)])
       +- Sort [continent#418 ASC NULLS FIRST, country#428 ASC NULLS FIRST], false, 0
          // Репартиционирование по continent + country на 200 партиций.
          +- Exchange hashpartitioning(continent#418, country#428, 200), ENSURE_REQUIREMENTS, [id=#498]
             // Удаление дубликатов внутри каждой партиции.
             +- SortAggregate(key=[continent#418, country#428], functions=[partial_first(name#209, false), partial_first(population#419L, false)])
                +- Sort [continent#418 ASC NULLS FIRST, country#428 ASC NULLS FIRST], false, 0
                   +- Project [coalesce(continent#207, Undefined) AS continent#418, CASE WHEN (country#208 = Rossiya) THEN Russia ELSE country#208 END AS country#428, name#209, coalesce(population#210L, 0) AS population#419L]
                      +- Filter atleastnnonnulls(1, continent#207, country#208, name#209, population#210L)
                         +- Scan ExistingRDD[continent#207,country#208,name#209,population#210L]
   */

  Thread.sleep(1000000)

  spark.stop
}
