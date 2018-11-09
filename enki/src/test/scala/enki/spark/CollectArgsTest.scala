package enki
package spark

import cats.implicits._
import enki.internal._

class CollectArgsTest extends EnkiTestSuite {
  "CollectArgs" should {
    "collect argument ops" in {
      def a[F[_]](implicit alg: SparkAlg[F]) = alg.arg("a1")

      a.analyze(CollectArgs(Set(_)).analyzer) shouldBe Set(Argument("a1"))
    }

    "collect arguments from plain sql" in {
      def a[F[_]](implicit alg: SparkAlg[F]) = alg.sql(f"select * from a where a.a = $${a1} and a.b = $${env:b1}")

      a.analyze(CollectArgs(Set(_)).analyzer) shouldBe  Set(Argument("a1"), Argument("env:b1"))
    }
  }
}
