package enki.pm.project

import cats._
import cats.data.Kleisli
import cats.implicits._
import enki.pm.internal._
import qq.droste._
import qq.droste.data._
import qq.droste.data.prelude._

/**
  * Generate qualified names for modules. Since it requires access to module coalgebra carrier it should be fused with
  * module coalgebra and applied early.
  */
case class QualifiedNameGenerator[F[_] : Functor, A](
                                                      moduleTreeBuilder: CoalgebraM[F, RoseTreeF[Validated[Module], ?], A],
                                                      moduleName: A => String
                                                    ) {
  type Tree = RoseTreeF[Validated[Module], A]

  private def run(a: (Option[String], A)): F[AttrRoseTreeF[String, Validated[Module], (Option[String], A)]] = {
    val qualifiedName = a._1 match  {
      case Some(parentName) => s"$parentName.${moduleName(a._2)}"
      case None => moduleName(a._2)
    }
    moduleTreeBuilder.run(a._2) fmap { res => AttrF(qualifiedName, res.fmap(c => (Some(qualifiedName), c))) }
  }

  def coalgebra: CoalgebraM[F, AttrRoseTreeF[String, Validated[Module], ?], (Option[String], A)] =
    CoalgebraM[F, AttrRoseTreeF[String, Validated[Module], ?], (Option[String], A)](run)
}