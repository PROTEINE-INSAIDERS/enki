package enki.dataset

import enki.tests.EnkiSuite
import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}

class SyntaxTest extends WordSpec with Matchers with EnkiSuite with Syntax {
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
