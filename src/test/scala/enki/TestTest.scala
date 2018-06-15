package enki

import cats.data._
import cats.implicits._
import enki.implicits._
import enki.sources.default._
import org.apache.spark.sql.DataFrame
import org.scalatest.{Matchers, WordSpec}

case class Enki()

case class Source1(a: Int, b: Int)

case class Source2(a: Int, b: String)

class TestTest extends WordSpec with Matchers {
  type EnkiReader[T] = Reader[Enki, T]

  "aaa" in {

    val a = source[(Int, Int)]('sourceA)
    val b = source[(Int, Int)]('sourceB)

    val c: Plan[DataFrame] = (a, b, session) mapN { (dsa, dsb, s) =>
      import s.implicits._
      dsa.as("a").join(dsb.as("b"), $"a._2" === $"b._1")
    }

    val d = stage('state1, c)

    // val d = (c).ap  { (d : DataFrame) => stage('testState, d) }

    Set("a") |+| Set("b")
    Map("a" -> 1) |+| Map("a" -> 2)

    implicit val s = LocalSparkSession.session

    createEmptySources(c)

    val res = eval(c)
    res.show()
  }
}
