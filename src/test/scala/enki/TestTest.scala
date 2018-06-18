package enki

import cats._
import cats.implicits._
import enki.implicits._
import enki.plan.SourceOp
import enki.sources.EmptySource
import enki.sources.default._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.scalatest.{Matchers, WordSpec}

class TestTest extends WordSpec with Matchers {
  "aaa" in {

    val a = source[Row]('testTable1)
    val b = source[(Int, Int)]('sourceB)

    val c: Plan[DataFrame] = (a, b, session) mapN { (dsa, dsb, s) =>
      import s.implicits._
      dsa.as("a").join(dsb.as("b"), $"a.c" === $"b._1")
    }

    implicit val s = LocalSparkSession.session
    val emptySource = new EmptySource {
      private val fromSource = schemaFromResource('schemas)

      override protected def getSchema(name: Symbol): Option[StructType] = fromSource(name)
    }

    val e = evaluator(s)

    val m = sourceMapper(λ[SourceOp ~> SourceOp] {
      case op: SourceOp[t] => SourceOp[t](op.name, emptySource)(op.typeTag)
    })

    val ee = e compose m

    val res = c foldMap ee
    res.show()
  }
}
