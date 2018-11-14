package enki.pm.project

import cats._
import cats.implicits._
import cats.mtl._
import enki.pm.internal._
import qq.droste._

case class InvalidModuleFilter[M[_] : Applicative](implicit invalids: FunctorTell[M, ValidationError]) {
  def algebra: AlgebraM[M, RoseTreeF[Validated[Module], ?], RoseTree[Module]] =
    AlgebraM[M, RoseTreeF[Validated[Module], ?], RoseTree[Module]] {
      case RoseTreeF.Leaf(Validated.Valid(module: Module)) => RoseTree.leaf(module).pure[M]
      case RoseTreeF.Leaf(Validated.Invalid(error)) => invalids.tell(error) fmap { _ => RoseTree.node[Module](List.empty) }
      case RoseTreeF.Node(xs: List[RoseTree[Module]]) => RoseTree.node[Module](xs.filter {
        case RoseTree.Node(xxs) if xxs.isEmpty => false
        case _ => true
      } ).pure[M]
    }
}