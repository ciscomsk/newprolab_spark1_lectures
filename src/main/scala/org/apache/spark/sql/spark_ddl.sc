import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

val spark: SparkSession =
  SparkSession
    .builder()
    .master("local[*]")
    .appName("l_9")
    .getOrCreate()

val sc: SparkContext = spark.sparkContext
sc.setLogLevel("ERROR")

spark
  .range(10)
  .withColumn("id", col("id").cast("int"))
  .withColumn("value", lit("foo"))
  .schema
  .toDDL