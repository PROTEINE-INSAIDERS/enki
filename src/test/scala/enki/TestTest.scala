package enki

import cats._
import cats.implicits._
import enki.tests.EnkiSuite
import org.apache.spark.sql._
import org.scalatest.{Matchers, WordSpec}
import enki.sources.default._
import scala.reflect.io.Path

class TestTest extends WordSpec with Matchers with EnkiSuite {
  "stages" in {


    val a = source[Row]('testTable1)

    val s1 = stage('s1) {
      a
    }

    val b = (d1: Dataset[Row], d2: Dataset[Row]) => {
      d1.crossJoin(d2)
    }

    val s2 = stage('s2) {
      (s1, s1) mapN b
    }

    val s3 = stage('s3) {
      (s1, s1) mapN b
    }

    /*
           s1
           | \
           s2 \
           |  /
           s3
     */

  }


  "aaa" in {

    val a = source[Row]('testTable1)
    val b = source[(Int, Int)]('sourceB)

    val c: Program[DataFrame] = (a, b, session) mapN { (dsa, dsb, s) =>
      import s.implicits._
      dsa.as("a").join(dsb.as("b"), $"a.c" === $"b._1")
    }

    val emptySource = new EmptySource with SchemaFromResource {
      override def root: Path = Path("/schemas")
    }

    val m = sourceMapper(λ[SourceSt ~> SourceSt] {
      case op: SourceSt[t] => SourceSt[t](op.name, emptySource)(op.typeTag)
    })

    val ee = evaluator compose m

    val res = c foldMap ee
    res.show()
  }
}
