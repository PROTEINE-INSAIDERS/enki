package enki

import cats._
import cats.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
/*
case class DecimalPrecisionTestData(@decimalPrecision(38, 12) a: Option[BigDecimal])

class StageTest extends EnkiTestSuite with Database[Stage.Op, simpleProgram.ProgramM.Op] {

  override  val s: enki.Stage[Stage.Op] = Stage[Stage.Op]
  override val p: enki.Program1[Stage.Op, enki.simpleProgram.ProgramM.Op] = simpleProgram.ProgramM[simpleProgram.ProgramM.Op]


  import implicits._

  override def schema: String = "default"

 // override def writerSettings[F[_]](implicit writer: enki.DataFrameWriter[F]): writer.FS[Unit] = writer.mode(SaveMode.Overwrite)

  override def encoderStyle: EncoderStyle = EncoderStyle.Enki

  "ReadAction" should {
    "handle non-default decimal's scale and precision" in {
      /*
      val schema = StructType(Array(StructField(name = "a", dataType = DecimalType(38, 12))))

      sparkSession.sqlContext.createDataFrame(Seq(Row(BigDecimal(10))), schema).write.mode(SaveMode.Overwrite).saveAsTable("default.DecimalPrecisionTestData")
      val r = read[DecimalPrecisionTestData](tableName = "DecimalPrecisionTestData")
      sparkSession.run(r).collect() shouldBe Array(DecimalPrecisionTestData(a = BigDecimal(10)))
    }

    "read untyped datasets" in {
      sparkSession.run {
        dataset(Seq((1, "test1", true), (2, "test2", false))) ap write[(Int, String, Boolean)]("t1")
      }

      val res = sparkSession.run {
        read("t1")
      }

      res.collect().sortBy(_.getInt(0)) shouldBe Array(
        Row(1, "test1", true),
        Row(2, "test2", false)
      )
      */
      ???
    }
  }

  "DatasetAction" should {
    "handle non-default decimal's scale and precision" in {
      ???
      /*
      val a = dataset[DecimalPrecisionTestData](Seq(DecimalPrecisionTestData(a = BigDecimal(10))))
      sparkSession.run(a).collect() shouldBe Array(DecimalPrecisionTestData(a = BigDecimal(10)))
      */
    }
  }
}
*/