package enki.datasets

import enki.AllModules
import enki.tests.EnkiSuite
import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}

class SyntaxTest extends WordSpec with Matchers with EnkiSuite {
  "diff" should {
    "format" in {
      import sparkSession.implicits._

      val a =
        sparkSession.createDataset(Seq((1, "a"), (2, "b"), (3, "c"))).toDF()
      val b =
        sparkSession.createDataset(Seq((2, "x"), (3, "c"), (4, "d"))).toDF()

      val c = a.diff(b, Seq("_1"))
      val d = formatDiff(c)

      d.collect().sortBy(_.getString(1)) shouldBe Array(
        Row("---", "1", "a"),
        Row("->", "2", "b->x"),
        Row(null, "3", "c"),
        Row("+++", "4", "d")
      )
    }
  }

  "fillna" should {
    "support datasets" in {
      import sparkSession.implicits._

      val ds = sparkSession.createDataset(
        Seq(
          ("a", Some(10)),
          ("b", None)
        ))

      ds.fillna(0).collect().sortBy(_._1) shouldBe Array(
        ("a", Some(10)),
        ("b", Some(0))
      )
    }

    "support dataframes" in {
      val df = sparkSession.createDataFrame(
        Seq(
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
