package enki.dataset

import enki.tests.EnkiSuite
import org.scalatest.{Matchers, WordSpec}

class SyntaxTest extends WordSpec with Matchers with EnkiSuite with Syntax {
  "fillna" in {
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
}
