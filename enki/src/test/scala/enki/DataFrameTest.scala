package enki

import enki.tests.EnkiSuite
import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}

class DataFrameTest extends WordSpec with Matchers with EnkiSuite {
  "diff" should {
    "format" in {
      import sparkSession.implicits._

      val a = sparkSession.createDataset(Seq((1, "a", 0.0), (2, "b", 0.0), (3, "c", 0.0))).toDF()
      val b = sparkSession.createDataset(Seq((2, "x", 0.0), (3, "c", 0.0), (4, "d", 0.0))).toDF()

      val c = a.diff(b, Seq("_1"), true)
      val d = formatDiff(c)

      d.collect().sortBy(_.getString(1)) shouldBe Array(
        Row("---", "1", "a", "0.0"),
        Row("->", "2", "b->x", "0.0"),
        Row(null, "3", "c", "0.0"),
        Row("+++", "4", "d", "0.0")
      )
    }
  }

  "fillna" should {
    "support datasets" in {
      import sparkSession.implicits._

      val ds = sparkSession.createDataset(Seq(
        ("a", Some(10)),
        ("b", None)
      ))

      ds.fillna(0).collect().sortBy(_._1) shouldBe Array(
        ("a", Some(10)),
        ("b", Some(0))
      )
    }

    "support dataframes" in {
      val df = sparkSession.createDataFrame(Seq(
        ("a", Some(10)),
        ("b", None)
      ))

      df.fillna(0).collect().sortBy(r => r.getString(0)) shouldBe Array(
        Row("a", 10),
        Row("b", 0)
      )
    }
  }
}
