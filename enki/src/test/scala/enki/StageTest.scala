package enki

import cats.implicits._
import enki.default._
import freestyle.free._
import freestyle.free.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

case class DecimalPrecisionTestData(@decimalPrecision(38, 12) a: Option[BigDecimal])

class StageTest extends EnkiTestSuite with enki.default.Database {

  import implicits._

  override def schema: String = "default"

  override def writerSettings: Stage[enki.WriterSettings] = WriterSettings().setMode(SaveMode.Overwrite).pure[Stage]

  override def encoderStyle: EncoderStyle = EncoderStyle.Enki

  "ReadAction" should {
    "handle non-default decimal's scale and precision" in {

      val schema = StructType(Array(StructField(name = "a", dataType = DecimalType(38, 12))))

      sparkSession.sqlContext.createDataFrame(Seq(Row(BigDecimal(10))), schema).write.mode(SaveMode.Overwrite).saveAsTable("default.DecimalPrecisionTestData")
      val r = read[DecimalPrecisionTestData](tableName = "DecimalPrecisionTestData")
      val res = r.interpret[EnkiMonad].run(Environment(sparkSession))

      res.collect() shouldBe Array(DecimalPrecisionTestData(a = BigDecimal(10)))
    }

    "read untyped datasets" in {
      val s1 = dataset(Seq((1, "test1", true), (2, "test2", false))) ap write[(Int, String, Boolean)]("t1")
      s1.interpret[EnkiMonad].run(Environment(sparkSession))
      val s2 = read("t1")
      val res = s2.interpret[EnkiMonad].run(Environment(sparkSession))

      res.collect().sortBy(_.getInt(0)) shouldBe Array(
        Row(1, "test1", true),
        Row(2, "test2", false)
      )

    }
  }

  "DatasetAction" should {
    "handle non-default decimal's scale and precision" in {
      val a = dataset[DecimalPrecisionTestData](Seq(DecimalPrecisionTestData(a = BigDecimal(10))))
      val res = a.interpret[EnkiMonad].run(Environment(sparkSession))

      res.collect() shouldBe Array(DecimalPrecisionTestData(a = BigDecimal(10)))
    }
  }
}
