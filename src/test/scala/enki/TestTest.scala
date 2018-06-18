package enki

import cats._
import cats.implicits._

import enki.implicits._
import enki.program.SourceSt
import enki.sources.EmptySource
import enki.sources.default._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.scalatest.{Matchers, WordSpec}

class TestTest extends WordSpec with Matchers {
  "stages" in {
    val a = source[Row]('testTable1)

    val s1 = stage('s1) { a }

    val b = (d1: Dataset[Row], d2: Dataset[Row]) => {
      d1.crossJoin(d2)
    }

    val s2 = stage('s2) { (s1, s1) mapN b }

    val s3 = stage('s3) { (s1, s1) mapN b }

    /*
           s1
           | \
           s2 \
           |  /
           s3
     */
    ???
  }


  "aaa" in {

    val a = source[Row]('testTable1)
    val b = source[(Int, Int)]('sourceB)

    val c: Program[DataFrame] = (a, b, session) mapN { (dsa, dsb, s) =>
      import s.implicits._
      dsa.as("a").join(dsb.as("b"), $"a.c" === $"b._1")
    }

    implicit val s = LocalSparkSession.session
    val emptySource = new EmptySource {
      private val fromSource = schemaFromResource('schemas)

      override protected def getSchema(name: Symbol): Option[StructType] = fromSource(name)
    }

    val e = evaluator(s)

    val m = sourceMapper(λ[SourceSt ~> SourceSt] {
      case op: SourceSt[t] => SourceSt[t](op.name, emptySource)(op.typeTag)
    })

    val ee = e compose m

    val res = c foldMap ee
    res.show()
  }
}
