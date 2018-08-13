package enki

import cats.implicits._
import enki.sparkImplicits._
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._

class ProgramTest extends EnkiTestSuite {
  "buildActionGraph" should {
    "detect dependencies" in {
      val p: Program[Stage[Unit]] = for {
        a <- persist("default", "a", dataset(Seq(1), strict = false, allowTruncate = false), strict = false, allowTruncate = false, None)
        b <- persist("default", "b", a, strict = false, allowTruncate = false, None)
        c <- persist("default", "c", a, strict = false, allowTruncate = false, None)
        d <- persist("default", "d", (c, b) mapN { (_, _) => sparkSession.emptyDataset[Int] }, strict = false, allowTruncate = false, None)
      } yield ().pure[Stage]

      val g = buildActionGraph("root", p)

      g.graph.edges.toOuter.toList.sortBy { case DiEdge(from, to) => (from, to) } shouldBe Seq(
        "default.b" ~> "default.a",
        "default.c" ~> "default.a",
        "default.d" ~> "default.b",
        "default.d" ~> "default.c")
    }

    "ignore empty stages" in {
      val p: Program[Stage[Unit]] = for {
        a <- persist("default", "a", dataset(Seq(1), strict = false, allowTruncate = false), strict = false, allowTruncate = false, None)
      } yield ().pure[Stage]

      val g = buildActionGraph("root", p)
      g.actions.keys.toSeq shouldBe Seq("default.a")
    }
  }
}