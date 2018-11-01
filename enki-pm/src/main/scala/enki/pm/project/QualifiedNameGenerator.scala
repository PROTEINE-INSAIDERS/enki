package enki.pm.project

import cats._
import cats.data.Kleisli
import cats.implicits._
import enki.pm.internal._
import qq.droste._
import qq.droste.data.prelude._

/**
  * Generate qualified names for modules. Since it requires access to module coalgebra carrier it should be fused with
  * module coalgebra and applied early.
  */
class QualifiedNameGenerator[F[_] : Functor, A](moduleTreeBuilder: CoalgebraM[F, RoseTreeF[Validated[Module], ?], A]) {
  type Tree = RoseTreeF[Validated[Module], A]

  def coalgebra: CoalgebraM[F, AttrRoseTreeF[String, Validated[Module], ?], (A, Option[String])] =
    CoalgebraM[F, AttrRoseTreeF[String, Validated[Module], ?], (A, Option[String])] {
      fa =>
        val k1 = new Kleisli[F, A, RoseTreeF[Validated[Module], A]](moduleTreeBuilder.run)


        def yoba(
                  a: Tuple2[A, Option[String]],
                  b: RoseTreeF[Validated[Module], A]
                ): AttrRoseTreeF[String, Validated[Module], (A, Option[String])] = {
          // тут есть доступ к параметру и к результату, можно мапнуть имена.
          ???
        }

        val k4 = k1
          .lmap[(A, Option[String])](_._1)
          .tapWith[AttrRoseTreeF[String, Validated[Module], (A, Option[String])]](yoba)

        val alg = CoalgebraM[F, AttrRoseTreeF[String, Validated[Module], ?], (A, Option[String])](k4.run)

        // val k3 = k1.lmap(_._1)

        // val k3 = k1.dimap[(A, Option[String]), Tree](_._1)(identity)


        // A = (A, Option[String])
        // B =
        //.tapWith[AttrRoseTreeF[String, Validated[Module], (A, Option[String])]] {
        //case fff =>
        ???
    }
}


object test {
  type SomeUnfixedTree[A] = RoseTreeF[Validated[Module], A]


  val f: Functor[SomeUnfixedTree] = implicitly

  val t1: SomeUnfixedTree[Int] = ???
  val t2 = f.fmap[Int, String](t1)(_.toString)
}