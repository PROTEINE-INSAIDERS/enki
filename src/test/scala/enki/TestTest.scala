package enki

import cats._
import cats.data._
import cats.implicits._
import enki.implicits._
import org.scalatest.{Matchers, WordSpec}

case class Enki()

class TestTest extends WordSpec with Matchers {
  type EnkiReader[T] = Reader[Enki, T]

  "aaa" in {
    import enki.configuration.default._

    val a = source[(Int, Int)]('sourceA)
    val b = source[(Int, Int)](name = 'sourceB)

    val c = (a, b, session) mapN { (dsa, dsb, s) =>
      import s.implicits._
      dsa.as("a").join(dsb.as("b"), $"a._2" === $"b._1")
    }

    implicit val s = LocalSparkSession.session

    createEmptySources(c)

    val res = eval(c)
    res.show()
  }
}
