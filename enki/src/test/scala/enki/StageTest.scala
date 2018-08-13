package enki

import cats.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

case class DecimalPrecisionTestData(@decimalPrecision(38, 12) a: Option[BigDecimal])

class StageTest extends EnkiTestSuite {
  "ReadAction" should {
    "handle non-default decimal's scale and precision in strict mode" in {

      val schema = StructType(Array(StructField(name = "a", dataType = DecimalType(38, 12))))

      sparkSession.sqlContext.createDataFrame(Seq(Row(BigDecimal(10))), schema).write.mode(SaveMode.Overwrite).saveAsTable("default.DecimalPrecisionTestData")
      val r = enki.read[DecimalPrecisionTestData](schemaName = "default", tableName = "DecimalPrecisionTestData", strict = true)
      r.foldMap(stageCompiler).apply(sparkSession).collect() shouldBe Array(DecimalPrecisionTestData(a = BigDecimal(10)))
    }
  }

  "DatasetAction" should {
    "handle non-default decimal's scale and precision in strict mode" in {
      val a = enki.dataset[DecimalPrecisionTestData](Seq(DecimalPrecisionTestData(a = BigDecimal(10))), strict = true, allowTruncate = true)
      a.foldMap(stageCompiler).apply(sparkSession).collect() shouldBe Array(DecimalPrecisionTestData(a = BigDecimal(10)))}
  }
}

