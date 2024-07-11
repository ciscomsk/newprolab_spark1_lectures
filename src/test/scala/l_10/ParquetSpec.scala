package l_10

import org.apache.spark.Partition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.lang

/** sbt_shell -> testOnly l_10.ParquetSpec */
class ParquetSpec extends AnyFlatSpec with should.Matchers with SparkSupport {
  "Parquet" should "be" in {
    val df: Dataset[lang.Long] = spark.range(0, 10_000, 1, 10)

//    df
//      .write
//      .mode(SaveMode.Overwrite)
//      .parquet("src/main/resources/l_10/test.parquet")

    val dataDf: DataFrame =
      spark
        .read
        .parquet("src/main/resources/l_10/test.parquet")

    println(dataDf.rdd.getNumPartitions)
    println()
    dataDf.printSchema()
    dataDf.show()

    val partitions: Array[Partition] =
      dataDf
        .rdd
        .partitions

    partitions.foreach(println)  // FilePartition(0,[Lorg.apache.spark.sql.execution.datasources.PartitionedFile;@7b6e196e)
    println()
    println(partitions.head.getClass.getCanonicalName)  // org.apache.spark.sql.execution.datasources.FilePartition
    println()

    val arr: Array[String] =
      partitions
        .map(_.asInstanceOf[FilePartition])
        .map(_.files.mkString("\n"))

    arr.foreach(println)
    // path: file:///home/mike/_learn/Spark/newprolab_1/_repos/lectures/src/main/resources/l_10/test.parquet/part-00005-3b6804ff-9473-4153-b1bb-b987d4099d31-c000.snappy.parquet, range: 0-4491, partition values: [empty row]
    println()

    println(arr.head.getClass.getCanonicalName) // java.lang.String
  }

}
