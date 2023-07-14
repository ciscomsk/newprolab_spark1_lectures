package l_5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.CodegenMode
import org.apache.spark.sql.execution.command.ExplainCommand
/** Spark 3.3.2 */
import org.apache.spark.sql.execution.ExtendedMode
/** 2.4.8 */
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrame_5 extends App {
  // не работает в Spark 3.4.0
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

  val csvOptions: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")

  val airportsDf: DataFrame =
    spark
      .read
      .options(csvOptions)
      .csv("src/main/resources/l_3/airport-codes.csv")

  /** План выполнения */

  println()
  airportsDf.printSchema()
  /** show - Physical Operator(PO) CollectLimit */
  airportsDf.show(numRows = 1, truncate = 100, vertical = true)

  airportsDf.explain(extended = true)
  /*
    == Parsed Logical Plan ==
    Relation [ident#17,type#18,name#19,elevation_ft#20,continent#21,iso_country#22,iso_region#23,municipality#24,gps_code#25,iata_code#26,local_code#27,coordinates#28] csv

    == Analyzed Logical Plan ==
    ident: string, type: string, name: string, elevation_ft: int, continent: string, iso_country: string, iso_region: string, municipality: string, gps_code: string, iata_code: string, local_code: string, coordinates: string
    Relation [ident#17,type#18,name#19,elevation_ft#20,continent#21,iso_country#22,iso_region#23,municipality#24,gps_code#25,iata_code#26,local_code#27,coordinates#28] csv

    == Optimized Logical Plan ==
    Relation [ident#17,type#18,name#19,elevation_ft#20,continent#21,iso_country#22,iso_region#23,municipality#24,gps_code#25,iata_code#26,local_code#27,coordinates#28] csv

    == Physical Plan ==
    // !!! Информация обрезается
    FileScan csv [ident#17,type#18,name#19,elevation_ft#20,continent#21,iso_country#22,iso_region#23,municipality#24,gps_code#25,iata_code#26,local_code#27,coordinates#28] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
   */

  /** Логирование плана выполнения */

  /** queryExecution - класс, представляющий собой дерево выполнения каких-то действий над данными */
  def printPhysicalPlan(ds: Dataset[_]): Unit = {
    /** !!! Информация обрезается */
    println(ds.queryExecution.executedPlan.treeString)
  }

  printPhysicalPlan(airportsDf)
  /*
    FileScan csv [ident#17,type#18,name#19,elevation_ft#20,continent#21,iso_country#22,iso_region#23,municipality#24,gps_code#25,iata_code#26,local_code#27,coordinates#28] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
   */

  /**
   * !!! executedPlan.toJSON - информация не обрезается
   * echo '<json>' | jq - удобный анализ в консоли
   */
  println(airportsDf.queryExecution.executedPlan.toJSON)
  /*
    [{"class":"org.apache.spark.sql.execution.FileSourceScanExec","num-children":0,"relation":null,"output":[[{"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference","num-children":0,"name":"ident","dataType":"string","nullable":true,"metadata":{},"exprId":{"product-class":"org.apache.spark.sql.catalyst.expressions.ExprId","id":17,"jvmId":"8d4776f9-6bf9-4f40-b370-6896a4f255d4"},"qualifier":[]}],[{"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference","num-children":0,"name":"type","dataType":"string","nullable":true,"metadata":{},"exprId":{"product-class":"org.apache.spark.sql.catalyst.expressions.ExprId","id":18,"jvmId":"8d4776f9-6bf9-4f40-b370-6896a4f255d4"},"qualifier":[]}],[{"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference","num-children":0,"name":"name","dataType":"string","nullable":true,"metadata":{},"exprId":{"product-class":"org.apache.spark.sql.catalyst.expressions.ExprId","id":19,"jvmId":"8d4776f9-6bf9-4f40-b370-6896a4f255d4"},"qualifier":[]}],[{"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference","num-children":0,"name":"elevation_ft","dataType":"integer","nullable":true,"metadata":{},"exprId":{"product-class":"org.apache.spark.sql.catalyst.expressions.ExprId","id":20,"jvmId":"8d4776f9-6bf9-4f40-b370-6896a4f255d4"},"qualifier":[]}],[{"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference","num-children":0,"name":"continent","dataType":"string","nullable":true,"metadata":{},"exprId":{"product-class":"org.apache.spark.sql.catalyst.expressions.ExprId","id":21,"jvmId":"8d4776f9-6bf9-4f40-b370-6896a4f255d4"},"qualifier":[]}],[{"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference","num-children":0,"name":"iso_country","dataType":"string","nullable":true,"metadata":{},"exprId":{"product-class":"org.apache.spark.sql.catalyst.expressions.ExprId","id":22,"jvmId":"8d4776f9-6bf9-4f40-b370-6896a4f255d4"},"qualifier":[]}],[{"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference","num-children":0,"name":"iso_region","dataType":"string","nullable":true,"metadata":{},"exprId":{"product-class":"org.apache.spark.sql.catalyst.expressions.ExprId","id":23,"jvmId":"8d4776f9-6bf9-4f40-b370-6896a4f255d4"},"qualifier":[]}],[{"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference","num-children":0,"name":"municipality","dataType":"string","nullable":true,"metadata":{},"exprId":{"product-class":"org.apache.spark.sql.catalyst.expressions.ExprId","id":24,"jvmId":"8d4776f9-6bf9-4f40-b370-6896a4f255d4"},"qualifier":[]}],[{"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference","num-children":0,"name":"gps_code","dataType":"string","nullable":true,"metadata":{},"exprId":{"product-class":"org.apache.spark.sql.catalyst.expressions.ExprId","id":25,"jvmId":"8d4776f9-6bf9-4f40-b370-6896a4f255d4"},"qualifier":[]}],[{"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference","num-children":0,"name":"iata_code","dataType":"string","nullable":true,"metadata":{},"exprId":{"product-class":"org.apache.spark.sql.catalyst.expressions.ExprId","id":26,"jvmId":"8d4776f9-6bf9-4f40-b370-6896a4f255d4"},"qualifier":[]}],[{"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference","num-children":0,"name":"local_code","dataType":"string","nullable":true,"metadata":{},"exprId":{"product-class":"org.apache.spark.sql.catalyst.expressions.ExprId","id":27,"jvmId":"8d4776f9-6bf9-4f40-b370-6896a4f255d4"},"qualifier":[]}],[{"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference","num-children":0,"name":"coordinates","dataType":"string","nullable":true,"metadata":{},"exprId":{"product-class":"org.apache.spark.sql.catalyst.expressions.ExprId","id":28,"jvmId":"8d4776f9-6bf9-4f40-b370-6896a4f255d4"},"qualifier":[]}]],"requiredSchema":{"type":"struct","fields":[{"name":"ident","type":"string","nullable":true,"metadata":{}},{"name":"type","type":"string","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"elevation_ft","type":"integer","nullable":true,"metadata":{}},{"name":"continent","type":"string","nullable":true,"metadata":{}},{"name":"iso_country","type":"string","nullable":true,"metadata":{}},{"name":"iso_region","type":"string","nullable":true,"metadata":{}},{"name":"municipality","type":"string","nullable":true,"metadata":{}},{"name":"gps_code","type":"string","nullable":true,"metadata":{}},{"name":"iata_code","type":"string","nullable":true,"metadata":{}},{"name":"local_code","type":"string","nullable":true,"metadata":{}},{"name":"coordinates","type":"string","nullable":true,"metadata":{}}]},"partitionFilters":[],"dataFilters":[],"disableBucketedScan":false}]
   */
  println()

  /** terminal => echo '<json>' | jq */
  /*
    [
      {
        "class": "org.apache.spark.sql.execution.FileSourceScanExec",
        "num-children": 0,
        "relation": null,
        "output": [
          [
            {
              "class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
              "num-children": 0,
              "name": "ident",
              "dataType": "string",
              "nullable": true,
              "metadata": {},
              "exprId": {
                "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
                "id": 17,
                "jvmId": "8d4776f9-6bf9-4f40-b370-6896a4f255d4"
              },
              "qualifier": []
            }
          ],
          [
            {
              "class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
              "num-children": 0,
              "name": "type",
              "dataType": "string",
              "nullable": true,
              "metadata": {},
              "exprId": {
                "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
                "id": 18,
                "jvmId": "8d4776f9-6bf9-4f40-b370-6896a4f255d4"
              },
              "qualifier": []
            }
          ],
          [
            {
              "class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
              "num-children": 0,
              "name": "name",
              "dataType": "string",
              "nullable": true,
              "metadata": {},
              "exprId": {
                "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
                "id": 19,
                "jvmId": "8d4776f9-6bf9-4f40-b370-6896a4f255d4"
              },
              "qualifier": []
            }
          ],
          [
            {
              "class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
              "num-children": 0,
              "name": "elevation_ft",
              "dataType": "integer",
              "nullable": true,
              "metadata": {},
              "exprId": {
                "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
                "id": 20,
                "jvmId": "8d4776f9-6bf9-4f40-b370-6896a4f255d4"
              },
              "qualifier": []
            }
          ],
          [
            {
              "class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
              "num-children": 0,
              "name": "continent",
              "dataType": "string",
              "nullable": true,
              "metadata": {},
              "exprId": {
                "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
                "id": 21,
                "jvmId": "8d4776f9-6bf9-4f40-b370-6896a4f255d4"
              },
              "qualifier": []
            }
          ],
          [
            {
              "class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
              "num-children": 0,
              "name": "iso_country",
              "dataType": "string",
              "nullable": true,
              "metadata": {},
              "exprId": {
                "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
                "id": 22,
                "jvmId": "8d4776f9-6bf9-4f40-b370-6896a4f255d4"
              },
              "qualifier": []
            }
          ],
          [
            {
              "class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
              "num-children": 0,
              "name": "iso_region",
              "dataType": "string",
              "nullable": true,
              "metadata": {},
              "exprId": {
                "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
                "id": 23,
                "jvmId": "8d4776f9-6bf9-4f40-b370-6896a4f255d4"
              },
              "qualifier": []
            }
          ],
          [
            {
              "class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
              "num-children": 0,
              "name": "municipality",
              "dataType": "string",
              "nullable": true,
              "metadata": {},
              "exprId": {
                "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
                "id": 24,
                "jvmId": "8d4776f9-6bf9-4f40-b370-6896a4f255d4"
              },
              "qualifier": []
            }
          ],
          [
            {
              "class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
              "num-children": 0,
              "name": "gps_code",
              "dataType": "string",
              "nullable": true,
              "metadata": {},
              "exprId": {
                "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
                "id": 25,
                "jvmId": "8d4776f9-6bf9-4f40-b370-6896a4f255d4"
              },
              "qualifier": []
            }
          ],
          [
            {
              "class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
              "num-children": 0,
              "name": "iata_code",
              "dataType": "string",
              "nullable": true,
              "metadata": {},
              "exprId": {
                "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
                "id": 26,
                "jvmId": "8d4776f9-6bf9-4f40-b370-6896a4f255d4"
              },
              "qualifier": []
            }
          ],
          [
            {
              "class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
              "num-children": 0,
              "name": "local_code",
              "dataType": "string",
              "nullable": true,
              "metadata": {},
              "exprId": {
                "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
                "id": 27,
                "jvmId": "8d4776f9-6bf9-4f40-b370-6896a4f255d4"
              },
              "qualifier": []
            }
          ],
          [
            {
              "class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
              "num-children": 0,
              "name": "coordinates",
              "dataType": "string",
              "nullable": true,
              "metadata": {},
              "exprId": {
                "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
                "id": 28,
                "jvmId": "8d4776f9-6bf9-4f40-b370-6896a4f255d4"
              },
              "qualifier": []
            }
          ]
        ],
        "requiredSchema": {
          "type": "struct",
          "fields": [
            {
              "name": "ident",
              "type": "string",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "type",
              "type": "string",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "name",
              "type": "string",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "elevation_ft",
              "type": "integer",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "continent",
              "type": "string",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "iso_country",
              "type": "string",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "iso_region",
              "type": "string",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "municipality",
              "type": "string",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "gps_code",
              "type": "string",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "iata_code",
              "type": "string",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "local_code",
              "type": "string",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "coordinates",
              "type": "string",
              "nullable": true,
              "metadata": {}
            }
          ]
        },
        "partitionFilters": [],
        "dataFilters": [],
        "disableBucketedScan": false
      }
    ]
   */

  /** filter == PO Filter - каталист добавляет isnotnull - высокопроизводительный фильтр (isnotnull часто можно запушдаунить в источник) */
  printPhysicalPlan(airportsDf.filter($"type" === "small_airport"))
  /*
   *(1) Filter (isnotnull(type#18) AND (type#18 = small_airport))
    +- FileScan csv [ident#17,type#18,name#19,elevation_ft#20,continent#21,iso_country#22,iso_region#23,municipality#24,gps_code#25,iata_code#26,local_code#27,coordinates#28] Batched: false, DataFilters: [isnotnull(type#18), (type#18 = small_airport)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [IsNotNull(type), EqualTo(type,small_airport)], ReadSchema: struct<ident:string,type:string,name:string,elevation_ft:int,continent:string,iso_country:string,...
   */

  /**
   * groupBy($"iso_country").count == 3 PO: 2 HashAggregate + Exchange hashpartitioning
   *
   * Первый HashAggregate содержит функцию partial_count(1) - это означает, что внутри каждой партиции будет выполнен
   * подсчет строк по каждому ключу (аналогично методу RDD reduceByKey)
   * output=[iso_country#22, count#120L] == создание маленького датафрейма внутри партиции после partial_count
   * Затем происходит репартиционирование (Exchange hashpartitioning) по ключу агрегата (iso_country),
   * после которого выполняется еще один HashAggregate с функцией count(1)
   *
   * Использование двух HashAggregate позволяет сократить количество передаваемых данных по сети
   */
  printPhysicalPlan(
    airportsDf
      .filter($"type" === "small_airport")
      .groupBy($"iso_country")
      .count()
  )
  /*
    AdaptiveSparkPlan isFinalPlan=false
    +- HashAggregate(keys=[iso_country#22], functions=[count(1)], output=[iso_country#22, count#116L])
       +- Exchange hashpartitioning(iso_country#22, 200), ENSURE_REQUIREMENTS, [plan_id=59]
          +- HashAggregate(keys=[iso_country#22], functions=[partial_count(1)], output=[iso_country#22, count#120L])
             +- Project [iso_country#22]
                +- Filter (isnotnull(type#18) AND (type#18 = small_airport))
                   +- FileScan csv [type#18,iso_country#22] Batched: false, DataFilters: [isnotnull(type#18), (type#18 = small_airport)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/reso..., PartitionFilters: [], PushedFilters: [IsNotNull(type), EqualTo(type,small_airport)], ReadSchema: struct<type:string,iso_country:string>
   */

  /** == ExtendedMode */
//  airportsDf
//    .filter($"type" === "small_airport")
//    .groupBy($"iso_country")
//    .count()
//    .explain(extended = true)


  val groupedDf: DataFrame =
  airportsDf
    .filter($"type" === "small_airport")
    .groupBy($"iso_country")
    .count()

  /** При необходимости можно прочитать java код, который был сгенерирован */
  def printCodeGen(ds: Dataset[_]): Unit = {
    val logicalPlan: LogicalPlan = ds.queryExecution.logical

    /** !!! В Spark 3+ - не работает - Found 0 WholeStageCodegen subtrees (?) */
    val codeGen: ExplainCommand =
      ExplainCommand(
        logicalPlan,
        CodegenMode // | ExtendedMode == df.explain(extended = true)
      )

    /** Spark 2.4.8 */
//    val codeGen: ExplainCommand =
//      ExplainCommand(
//        logicalPlan,
//        extended = true,
//        codegen = true
//      )

    spark
      .sessionState
      .executePlan(codeGen)
      .executedPlan
      .executeCollect()
      .foreach { intRow =>
        println(intRow.getString(0))
      }
  }

  /**
   * Сгенерированный java код сначала компилируется на драйвере (проверка синтаксиса кода),
   * потом передается на каждый воркер и компилируется уже там
   */
  printCodeGen(groupedDf)

  /** !!! PO ExchangeSinglePartition - снижает параллелизм до 1 партиции */


  println(sc.uiWebUrl)
  Thread.sleep(1000000)

  spark.stop()
}
