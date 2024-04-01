package l_4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.{col, explode, expr, length, lit, lower, split, struct, upper, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.lang

object DataFrame_1 extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("DataFrame_1")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("ERROR")

  import spark.implicits._

  val cityList: Vector[String] = Vector("Moscow", "Paris", "Madrid", "London", "New York")
  val df: DataFrame = cityList.toDF() // toDF - import spark.implicits._
  df.printSchema()
  /*
    root
     |-- value: string (nullable = true)
   */

  /** df.show работает аналогично rdd.take - пытается взять данные из минимального количества партиций => оптимизация */
  df.show()

  // vertical = true - вертикальная ориентация удобна при большом количестве колонок или длинной строке в колонке
  df.show(numRows = 20, truncate = 100, vertical = true)


  /**
   * алгоритм работы df.count:
   * 1. рассчитывается количество элементов в каждой партиции
   * 2. агрегированные данные пересылаются в одну партицию (Exchange single partition) - где производится финальный reduce
   * 3. результат передается на драйвер
   */
  println(df.count())
  println()

  /**
   * filter == СРЕЗ (PO Filter)
   * в отличии от RDD API - может принимать SQL выражение
   *
   * def filter(condition: Column): Dataset[T]
   */


  /** обращение к объекту Column */
  /**
   * v1 - $
   * $ также позволяет указывать колонки внутри структур
   */
  df
    .filter($"value" === "Moscow")
    .show()

  val df2: Dataset[lang.Long] = spark.range(10)
  df2.show()

  val df3: DataFrame =
    df2.
      select(
        struct(
          col("id").as("id_1"),
          col("id").as("id_2")
        ).as("foo")
      )

  df3.show()
  df3.printSchema()
  /*
    root
     |-- foo: struct (nullable = false)
     |    |-- id_1: long (nullable = false)
     |    |-- id_2: long (nullable = false)
   */

  df3
    .select($"foo.id_1")
    .show()

  df3
    .select($"foo.*") // foo.* - выбрать все колонки из структуры foo
    .show()

  println("df3.toJSON.show():")
  df3
    .toJSON
    .show(truncate = false)


  /** v2 - col() - классический API без синтаксического сахара */
  df
    .filter(col("value") === "Moscow")
    .show()
  // ==
  val moscow: Column = col("value") === "Moscow"
  df
    .filter(moscow)
    .show()

  /**
   * v3 - DML в SQL-like формате
   * !!! легко ошибиться + ошибка обнаружится только в рантайме
   */
  df
    .filter("value = 'Moscow'")
    .show()

  /**
   * v4 - expr - промежуточный вариант между col и обычной строкой
   * expr также используется для вызова SQL builtin функций, отсутствующих в org.apache.sql.functions
   */
  df
    .filter(expr("value = 'Moscow'"))
    .show()

  df
    .localCheckpoint()
    .filter(expr("value = 'Moscow'"))
    .explain()
  /*
    == Physical Plan ==
    *(1) Filter (isnotnull(value#1) AND (value#1 = Moscow))
    +- *(1) Scan ExistingRDD[value#1]
   */

  /**
   * Catalyst парсит выражения и создает план из физических операторов
   * -> план передается в Tungsten для кодогенерации
   */


  /** withColumn/select/drop == ПРОЕКЦИЯ (PO Project) */

  /**
   * withColumn - добавление новой колонку
   * является трансформацией => создает новый датафрейм (а не изменяет исходный)
   */
  df
    .withColumn("upperCity", upper($"value"))
    .show()

  df
    .localCheckpoint()
    .withColumn("upperCity", upper($"value"))
    .explain()
  /*
    == Physical Plan ==
    *(1) Project [value#1, upper(value#1) AS upperCity#107]
    +- *(1) Scan ExistingRDD[value#1]
   */


  /**
   * select - может использоваться не только для выборки существующих колонок, но и для СОЗДАНИЯ новых
   *
   * select(col("*")) - позволяет получить DF со всеми колонками - полезно, когда список всех колонок не известен
   * и нужно выбрать все существующие + добавить новые колонки
   *
   * в select можно передать список колонок, используя обычные строки
   */
  val withUpperDf: DataFrame = df.select($"value", upper($"value").as("upperCity"))
  withUpperDf.show()

  df
    .localCheckpoint()
    .select($"value", upper($"value").as("upperCity"))
    .explain()
  /*
    == Physical Plan ==
    *(1) Project [value#1, upper(value#1) AS upperCity#123]
    +- *(1) Scan ExistingRDD[value#1]
   */

  val myCols: List[Column] =
    List(
      col("value"),
      lit("foo"),
      lit(false),
      struct(col("value").as("woo")).as("moo")
    )

  df
    .select(myCols: _*)
    .printSchema()
  /*
    root
     |-- value: string (nullable = true)
     |-- foo: string (nullable = false)
     |-- false: boolean (nullable = false)
     |-- moo: struct (nullable = false)
     |    |-- woo: string (nullable = true)
   */

  /** alias == name == as */
  val multiSelectDf: DataFrame =
    withUpperDf
      .localCheckpoint()
      .select(
        col("*"),
        lower($"value").name("lowerCity"),
        (length($"value") + 1).as("length"),
        lit("foo").alias("bar")
      )

  multiSelectDf.printSchema()
  /*
    root
     |-- value: string (nullable = true)
     |-- upperCity: string (nullable = true)
     |-- lowerCity: string (nullable = true)
     |-- length: integer (nullable = true)
     |-- bar: string (nullable = false)
   */

  multiSelectDf.show()
  multiSelectDf.explain()
  /*
    == Physical Plan ==
    *(1) Project [value#1, upperCity#110, lower(value#1) AS lowerCity#136, (length(value#1) + 1) AS length#137, foo AS bar#138]
    +- *(1) Scan ExistingRDD[value#1,upperCity#110]
   */

  withUpperDf
    .select("value", "upperCity")
    .show()

  /**
   * drop - удаление колонок
   * !!! drop не выбросит исключение, если указана несуществующая колонка
   */
  withUpperDf
    .drop("upperCity", "abraKadabra")
    .show()

  withUpperDf
    .localCheckpoint()
    .drop("upperCity", "abraKadabra")
    .explain()
  /*
    == Physical Plan ==
    *(1) Project [value#1]
    +- *(1) Scan ExistingRDD[value#1,upperCity#110]
   */


  /** and/or/between/isin conditions */
  df
    .filter($"value" === "Moscow" or $"value" === "Paris") // == $"value".===("Moscow").or($"value".===("Paris"))
    .show()

  df
    .filter($"value".isin("Moscow", "Paris"))
    .show()


  /** Data cleaning */
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

  val rawDf: DataFrame =
    spark
      .range(0, 1)
      .select(lit(testData).as("value"))

  println("rawDf: ")
  rawDf.show(truncate = false)

  val jsonDf: DataFrame =
    rawDf.select(split(col("value"), "\n").as("value"))

  println("jsonDf: ")
  jsonDf.show(truncate = false)
  
  val jsonStrings: Column = split(col("value"), "\n").as("value")

  val splittedDs: Dataset[String] =
    rawDf
      .select(explode(jsonStrings))
      .as[String]

  println("splittedDs: ")
  splittedDs.show(numRows = 10, truncate = false)

  /** !!! spark.read.json позволяет читать не только json-файлы, но и Dataset[String], содержащие JSON строки */
  val df4: DataFrame =
    spark
      .read
      .json(splittedDs)

  println("df4: ")
  df4.show(truncate = false)
  df4.printSchema()
  /*
    root
     |-- _corrupt_record: string (nullable = true)
     |-- continent: string (nullable = true)
     |-- country: string (nullable = true)
     |-- name: string (nullable = true)
     |-- population: long (nullable = true)
   */

  val corruptedData: Array[Row] =
    df4
      .select(col("_corrupt_record"))
      .na.drop("all")
      .collect()

  println("corruptedData: ")
  println(corruptedData.mkString("Array(", ", ", ")"))
  println()

  val fillData: Map[String, Any] = Map("continent" -> "Undefined", "population" -> 0)
  val replaceData: Map[String, String] = Map("Rossiya" -> "Russia")

  val cleanDataDs: Dataset[Row] =
    df4
      .drop(col("_corrupt_record")) // drop - это проекция
      /**
       * .na.drop("all") - удаляются строки, где все колонки == null
       * .na.drop("any") - удаляются строки, где хотя бы одна колонка == null
       * можно указать на какие колонки будет распространяться это поведение (all/any)
       */
      .na.drop("all") // na.drop - это срез
      .na.fill(fillData) // na.fill - это проекция
      .na.replace("country", replaceData) // na.replace - это проекция
      /**
       * dropDuplicates без аргументов == distinct
       * dropDuplicates неявно запускает шафл
       */
      .dropDuplicates("continent", "country")

//  cleanDataDs
//    .repartition(1)
//    .write
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/l_4/cleandata")

  cleanDataDs.show()

  df4
    .drop(col("_corrupt_record"))
    .localCheckpoint()
    .na.drop("all")
    .explain()
  /*
    == Physical Plan ==
    *(1) Filter atleastnnonnulls(1, continent#224, country#225, name#226, population#227L)
    +- *(1) Scan ExistingRDD[continent#224,country#225,name#226,population#227L]
   */

  df4
    .drop(col("_corrupt_record"))
    .localCheckpoint()
    .na.drop("all")
    .na.fill(fillData)
    .explain()
  /*
    == Physical Plan ==
    *(1) Project [coalesce(continent#224, Undefined) AS continent#350, country#225, name#226, coalesce(population#227L, 0) AS population#351L]
    +- *(1) Filter atleastnnonnulls(1, continent#224, country#225, name#226, population#227L)
       +- *(1) Scan ExistingRDD[continent#224,country#225,name#226,population#227L]
   */

  df4
    .drop(col("_corrupt_record"))
    .localCheckpoint()
    .na.drop("all")
    .na.fill(fillData)
    .na.replace("country", replaceData)
    .explain()
  /*
    == Physical Plan ==
    *(1) Project [coalesce(continent#224, Undefined) AS continent#372, CASE WHEN (country#225 = Rossiya) THEN Russia ELSE country#225 END AS country#382, name#226, coalesce(population#227L, 0) AS population#373L]
    +- *(1) Filter atleastnnonnulls(1, continent#224, country#225, name#226, population#227L)
       +- *(1) Scan ExistingRDD[continent#224,country#225,name#226,population#227L]
   */

  df4
    .drop(col("_corrupt_record"))
    .localCheckpoint()
    .na.drop("all")
    .na.fill(fillData)
    .na.replace("country", replaceData)
    /** dropDuplicates - запускает шафл (Exchange hashpartitioning) */
    .dropDuplicates("continent", "country")
    .explain()
//    .explain(extended = true)
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    // удаление дубликатов внутри каждой партиции после репартицирования
    +- SortAggregate(key=[continent#403, country#413], functions=[first(name#226, false), first(population#404L, false)])
       +- Sort [continent#403 ASC NULLS FIRST, country#413 ASC NULLS FIRST], false, 0
          // репартиционирование по ключам continent + country на 200 партиций
          +- Exchange hashpartitioning(continent#403, country#413, 200), ENSURE_REQUIREMENTS, [plan_id=424]
             // удаление дубликатов внутри каждой партиции
             +- SortAggregate(key=[continent#403, country#413], functions=[partial_first(name#226, false), partial_first(population#404L, false)])
                +- Sort [continent#403 ASC NULLS FIRST, country#413 ASC NULLS FIRST], false, 0
                   +- Project [coalesce(continent#224, Undefined) AS continent#403, CASE WHEN (country#225 = Rossiya) THEN Russia ELSE country#225 END AS country#413, name#226, coalesce(population#227L, 0) AS population#404L]
                      +- Filter atleastnnonnulls(1, continent#224, country#225, name#226, population#227L)
                         +- Scan ExistingRDD[continent#224,country#225,name#226,population#227L]
   */

  /** when == SQL CASE WHEN */
  val newCol: Column =
    when(col("continent") === "Europe", lit(0))
      .when(col("continent") === "Africa", lit(1))
      /**
       * otherwise - дефолтное значение для остальных случаев
       * !!! если otherwise не указан - дефолтное значение будет NULL
       */
      .otherwise(lit(2))

  val whenDf: DataFrame =
    cleanDataDs
      .localCheckpoint()
      .withColumn("newCol", newCol)

  whenDf.show()
  whenDf.explain()
  /*
    == Physical Plan ==
    *(1) Project [continent#270, country#280, name#226, population#271L, CASE WHEN (continent#270 = Europe) THEN 0 WHEN (continent#270 = Africa) THEN 1 ELSE 2 END AS newCol#454]
    +- *(1) Scan ExistingRDD[continent#270,country#280,name#226,population#271L]
   */

  val constCol: Column = lit(3)
  val constColExpr: Expression = constCol.expr
  println(constColExpr) // == 3

  val constColExprJson: String = constCol.expr.toJSON
  println(constColExprJson)
  // == [{"class":"org.apache.spark.sql.catalyst.expressions.Literal","num-children":0,"value":"3","dataType":"integer"}
  println()


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
