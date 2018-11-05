package enki.pm.internal

import cats._
import cats.implicits._
import qq.droste._
import qq.droste.data._

object Attributes {
  def inheritedAttributes[M[_] : Functor : Semigroupal, F[_] : Functor, A, B](
                                                                               coalgebra: CoalgebraM[M, F, A],
                                                                               attributes: (A, B) => M[B]
                                                                             ): CoalgebraM[M, AttrF[F, B, ?], (A, B)] =
    CoalgebraM[M, AttrF[F, B, ?], (A, B)] { carrier =>
      (coalgebra.run(carrier._1), attributes(carrier._1, carrier._2)) mapN { (fa, b) => AttrF(b, fa fmap { a => (a, b) }) }
    }
}
