package enki

import cats._
import cats.implicits._
import enki.default._
import freestyle.free.FreeS._
import freestyle.free._
import freestyle.free.implicits._
import org.apache.spark.sql._
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._


class ProgramTest extends EnkiTestSuite with enki.default.Database {

  import implicits._

  override def schema: String = "default"

  override def writerSettings: Stage[enki.WriterSettings] = WriterSettings().setMode(SaveMode.Overwrite).pure[Stage]

  "buildActionGraph" should {
    "detect dependencies" in {
      val p = for {
        a <- persist("a", dataset(Seq(1)))
        b <- persist("b", a)
        c <- persist("c", a)
        d <- persist("d", (c, b) mapN { (_, _) => sparkSession.emptyDataset[Int] })
      } yield ().pure[Stage]

      val g = buildActionGraph("root", p)

      g.graph.edges.toOuter.toList.sortBy { case DiEdge(from, to) => (from, to) } shouldBe Seq(
        "default.b" ~> "default.a",
        "default.c" ~> "default.a",
        "default.d" ~> "default.b",
        "default.d" ~> "default.c")
    }

    "ignore empty stages" in {
      val p: FreeS[ProgramOp, Par[StageOp, Unit]] = for {
        a <- persist("a", dataset(Seq(1)))
      } yield ().pure[Stage]

      val g = buildActionGraph("root", p)
      g.actions.keys.toSeq shouldBe Seq("default.a")
    }
  }
}
