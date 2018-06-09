package enki

import cats._
import cats.implicits._
import enki.builder.ExecutionPlanBuilder
import enki.implicits._
import org.scalatest.{Matchers, WordSpec}

class TestTest extends WordSpec with Matchers {
  "testtest" in {
    val spark = LocalSparkSession.session

    val a = Applicative[ExecutionPlanBuilder].pure(1)
    val b = Applicative[ExecutionPlanBuilder].pure(2)
    val c = Applicative[ExecutionPlanBuilder].pure(2)
    val f = (k: Int) => (l: Int) => (m: Int) => k + l + m

    val x = a.fmap(f) <*> b <*> c

    val g = (k: Int, m: Int, n: Int) => k + m + n

    val y = (a, b, c) mapN g

    val z = syntaxTest
  }

  "todo" in {
    val s1 = source('someShit1)
    val s2 = source('someShit2)
    val s3 = Applicative[ExecutionPlanBuilder].pure("not even a result")

    
  }
}
