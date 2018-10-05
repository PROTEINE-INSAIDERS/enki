package enki

import cats.implicits._
import enki.default._
import freestyle.free._
import freestyle.free.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

class DatabaseTest extends EnkiTestSuite {
  "EncoderStyle" should {
    "be passed to implicits object" in {
      val database1 = new enki.default.Database {
        override def encoderStyle: EncoderStyle = EncoderStyle.Spark

        def schema = ""
      }

      val database2 = new enki.default.Database {
        override def encoderStyle: EncoderStyle = EncoderStyle.Enki

        def schema = ""
      }

      database1.implicits.encoderStyle shouldBe EncoderStyle.Spark
      database2.implicits.encoderStyle shouldBe EncoderStyle.Enki
    }
  }

  "read" should {
    "preserve decimal's precision in Enki mode" in {
      val schema = StructType(Array(StructField(name = "a", dataType = DecimalType(38, 12))))
      sparkSession.sqlContext.createDataFrame(Seq(Row(BigDecimal(10))), schema)
        .write.mode(SaveMode.Overwrite).saveAsTable("default.table1")

      val database1 = new enki.default.Database {
        val schema = "default"

        import implicits._

        override def encoderStyle: EncoderStyle = EncoderStyle.Enki

        val test = read[DecimalPrecisionTestData]("table1", strict = true)

        val enc = implicitly[Encoder[DecimalPrecisionTestData]]
      }

      val e = database1.enc

      println(e)

      database1.test.interpret[EnkiMonad].run(Environment(sparkSession))
      val res = sparkSession.sql("select * from default.table1").schema

      res.fields.foreach(println)
    }
  }

  "write" should {
    "preserve decimal's precision in Enki mode" in {
      val database1 = new enki.default.Database {
        def schema = "default"

        import implicits._

        override def writerSettings: stageAlg.FS[enki.WriterSettings] = super.writerSettings map (_.setMode(SaveMode.Overwrite))

        override def encoderStyle: EncoderStyle = EncoderStyle.Enki

        val test: Stage[Unit] = write[DecimalPrecisionTestData]("table1") <*> dataset[DecimalPrecisionTestData](Seq.empty)
      }

      database1.test.interpret[EnkiMonad].run(Environment(sparkSession))
      val schema = sparkSession.sql("select * from default.table1").schema

      schema shouldBe StructType(Seq(StructField(name = "a", dataType = DecimalType(38, 12))))
    }
  }

  "persist" should {
    "preserve decimal's precision in Enki mode" in {
      val database1 = new enki.default.Database {
        def schema = "default"

        import implicits._

        override def writerSettings: stageAlg.FS[enki.WriterSettings] = super.writerSettings map (_.setMode(SaveMode.Overwrite))

        override def encoderStyle: EncoderStyle = EncoderStyle.Enki

        val test: ProgramS[Stage[Unit]] = for {
          _ <- persist[DecimalPrecisionTestData]("table1", dataset[DecimalPrecisionTestData](Seq.empty), true)
        } yield ().pure[Stage]
      }

      val ag = buildActionGraph("root", database1.test)

      ag.runAll(stageHandler, Environment(sparkSession))

      val schema = sparkSession.sql("select * from default.table1").schema

      schema shouldBe StructType(Seq(StructField(name = "a", dataType = DecimalType(38, 12))))
    }
  }
}
