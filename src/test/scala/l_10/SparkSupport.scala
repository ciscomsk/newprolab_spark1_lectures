package l_10

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait SparkSupport {
  /** lazy - чтобы сессия создавалась только при обращении */
  lazy val spark: SparkSession =
    SparkSession
      .getActiveSession
      .getOrElse(SparkSession.builder().master("local[*]").appName("test").getOrCreate())

  lazy val sc: SparkContext = spark.sparkContext
}
