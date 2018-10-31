package enki.pm.project

import enki.pm.internal._
import cats._
import cats.implicits._
import org.apache.spark.sql.catalyst._
import qq.droste._
import qq.droste.data._

case class SynthesizedAttributes2[F[_]](
                                         isEmpty: F[Boolean],
                                         reads: F[Set[TableIdentifier]]
                                       )

class SynthesizedAttributesAlg[F[_] : Applicative] {
  type Op = Either[Module, List[SynthesizedAttributes2[F]]]

  type ModuleTreeF[A] = AttrRoseTreeF[InheritedAttributes, Module, A]

  protected def isEmpty(attr: InheritedAttributes, op: Op): F[Boolean] = op match {
    case Left(_) => false.pure[F]
    case Right(xs) => imply(all, InvariantMonoidal.monoid[F, Boolean]) {
      xs.map(_.isEmpty).combineAll
    }
  }

  protected def isEmpty(attr: InheritedAttributes, module: Module): F[Boolean] = false.pure[F]

  def alg: Algebra[ModuleTreeF, SynthesizedAttributes2[F]] = Algebra[ModuleTreeF, SynthesizedAttributes2[F]] {
    case AttrF(attr, RoseTreeF.Left(module: Module)) =>
      val a = isEmpty(attr, module)
      ???

      /*
      val unded = CoattrF.un[List, Module, SynthesizedAttributes2[F]](lower)

      lower match {
        case Left(m: Module) => ???
        case Right(l: List[SynthesizedAttributes2[F]]) => ???
      }
*/
    //val (attr, op) = AttrF.un(fa)
    // val k = isEmpty(attr, op)
    // case (attr: InheritedAttributes, op: Op) => ???

    /*
    case (attr: InheritedAttributes, Left(module: Module)) => SynthesizedAttributes2(
      isEmpty = isEmpty(attr, module),
      reads = ???
    )
    case (attr: InheritedAttributes, Right(submodules: List[SynthesizedAttributes2[F]])) => ???
    */
  }

  protected def isEmpty(attr: InheritedAttributes, submodules: List[SynthesizedAttributes2[F]]) =
    imply(all, InvariantMonoidal.monoid[F, Boolean]) {
      submodules.map(_.isEmpty).combineAll
    }
}
