package l_10

import org.apache.spark.sql.Dataset
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.lang

class SparkSpec extends AnyFlatSpec with should.Matchers with SparkSupport {
  /** sbt_shell -> testOnly l_10.SparkSpec */
  "spark" should "work" in {
    val df: Dataset[lang.Long] = spark.range(10)
    df.show()
  }
}
