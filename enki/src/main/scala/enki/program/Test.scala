package enki.program

import cats.Functor
import cats.free.Cofree
import cats.implicits._
import qq.droste.data._
import qq.droste.{Algebra, _}

sealed abstract class P[A]

final case class A1[A](value: Long) extends P[A]

final case class A2[A](values: List[A]) extends P[A]

object Test {
  implicit val f: Functor[P] = new Functor[P] {
    override def map[A, B](fa: P[A])(f: A => B): P[B] = fa match {
      case A1(a) => A1(a)
      case A2(ax) => A2(ax.map(f))
    }
  }

  type Test1[A] = Cofree[P, A]

  val testAlg: Algebra[P, Long] = Algebra {
    case A1(l) => l
    case A2(values) => values.sum
  }

  val natCoalgebra: Coalgebra[Option, BigDecimal] =
    Coalgebra(n => if (n > 0) Some(n - 1) else None)

  val fibAlgebra: CVAlgebra[Option, BigDecimal] = CVAlgebra {
    case Some(r1 :< Some(r2 :< _)) => r1 + r2
    case Some(_ :< None) => 1
    case None => 0
  }

  type Test2 = Fix[P]

  val k: Test2 = Fix(A2(List(Fix(A1(2L)), Fix(A1(1L)))))

  val aaa = scheme.cata(testAlg)
  val bbb = aaa(k)


  val fib: BigDecimal => BigDecimal = scheme.ghylo(
    fibAlgebra.gather(Gather.histo),
    natCoalgebra.scatter(Scatter.ana))
}

object Main {
  def main(args: Array[String]): Unit = {
    // println( Test.aaa)
  }
}
