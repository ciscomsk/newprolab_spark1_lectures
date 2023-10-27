package playground

import org.apache.spark.SparkContext
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, lit, struct, to_date}
import org.apache.spark.sql.types.TimestampType

import java.time.YearMonth
import scala.util.{Failure, Success, Try}

object Test extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("l_5")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  val data: Seq[(String, java.lang.Long)] =
    Seq(
      ("Java", 20000L),
      ("Python", 100000L),
      ("Scala", 3000L),
      ("Golang", 0L),
      ("", null.asInstanceOf[java.lang.Long]),
    )

//  val dataDF = data.toDF("lang", "sum")
//  dataDF.show()
//  dataDF.printSchema()

//  dataDF
//    .filter(col("sum") =!= lit("0"))
//    .filter(col("lang") =!= lit(""))
//    .filter(col("sum") =!= lit("0") and col("sum") =!= lit(""))
//    .show()



  val data2: Seq[(String, java.lang.Long)] =
  Seq(
    ("Mike", 3),
    ("Mike", 4),
    ("Mike", 5),
    ("Sven", 3),
    ("Sven", 3),
    ("Sven", 5),
    ("Sven", 5),
  )

//  val dataDF2 = data2.toDF("id", "mark")
//  dataDF2.show()
//
//  dataDF2
//    .groupBy("id", "mark")
//    .count()
//    .show()


  val simpleData: Seq[(String, String, Int)] =
    Seq(
      ("xxx",     "Sales", 3000),
      ("James",   "Sales", 3000),
      ("Robert",  "Sales", 4100),
      ("Saif",    "Sales", 4100),
      ("Michael", "Sales", 4600),
      ("Maria",   "Finance", 3000),
      ("Scott",   "Finance", 3300),
      ("Jen",     "Finance", 3900),
      ("Kumar",   "Marketing", 2000),
      ("Jeff",    "Marketing", 3000)
    )
//  val df = simpleData.toDF("employee_name", "department", "salary")
//  println(df.count())
//  df.show()

//  df
//    .withColumn("str", struct("department", "salary"))
//    .filter("str.salary IS NOT NULL").show()

//  val dropDisDF = df.dropDuplicates("department","salary")
//  println(dropDisDF.count())
//  dropDisDF.show()

  val ts: Seq[(Long, String)] =
    Seq(
      (1675167853L, "2003-01-31"),
      (1675254253L, "2023-02-01"),
      (1677587053L, "2023-02-28"),
      (1677673453L, "2023-03-01")
    )

  val tsDF = ts.toDF("ts", "date")
//  tsDF.show()
//  tsDF.printSchema()

  val castTs = tsDF.withColumn("ts", col("ts").cast(TimestampType))
//  castTs.show()
//  castTs.printSchema()

  val dateTs = castTs.withColumn("ts", to_date(col("ts")))
//  dateTs.show()
//  dateTs.printSchema()

  val ym = YearMonth.of(2023, 2)
  println(ym)
//  println(ym)
//  println(ym.atEndOfMonth())
//  println(ym.atEndOfMonth().getDayOfMonth)

//  val periodCondition = col("ts").between("2023-02-01", "2023-02-31")
//  val periodCondition = col("ts").between("2023-02-01", "2023-02-28")
//  val periodCondition = col("ts").between("2023-02-01", s"2023-02-${ym.atEndOfMonth().getDayOfMonth}")
//  val periodCondition = date_format(col("ts"), "yyyyMM") === ym
//  val periodCondition =  date_format(col("ts"), "yyyy-MM") === ym
//  val periodCondition =  date_format(col("ts"), "yyyy-MM") === "2023-02"
  val periodCondition =  date_format(col("ts"), "yyyy-MM") === ym.toString

//  dateTs.withColumn("ts", date_format(col("ts"), "yyyy-MM")).show()
//  dateTs.filter(periodCondition).show()

  val paths =
    Seq(
      "/home/mike/Downloads/part-00000-206d31df-db89-4c26-844e-b8c77a6bac13-c000.snappy.parquet",
      "/home/mike/Downloads/part-00000-206d31df-db89-4c26-844e-b8c77a6bac13-c002.snappy.parquet",
//      "/home/mike/Downloads/photo.txt"
    )

  val res: Seq[Try[DataFrame]] = paths.map { path =>
//    spark.read.parquet(path)
    Try(spark.read.parquet(path))
  }

  val result: Seq[DataFrame] = res.map {
    case Success(df) => df
    case Failure(ex: AnalysisException) if ex.message.contains("PATH_NOT_FOUND") =>
      println(ex.message)
      spark.emptyDataFrame
    case Failure(ex) => throw ex
  }

  result.filter(!_.isEmpty).reduce((df1, df2) => df1.union(df2)).show()

  // dataPathNotExistError




  println()



//  val succ = res.collect { case res if res.isSuccess => res.get }
//  println(succ)
//  succ.tail.foldLeft(succ.head)((acc, el) => acc.union(el)).show()



}
