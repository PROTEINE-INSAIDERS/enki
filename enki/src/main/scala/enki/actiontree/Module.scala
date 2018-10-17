package enki
package actiontree

import cats.free._
import cats.implicits._
import freestyle.free._
import qq.droste._
import qq.droste.data.Fix
import scalax.collection.GraphEdge._
import scalax.collection._

case class ActionDependencies(
                               unresolvedReads: List[String],
                               writes: List[String],
                               innerDependencies: Graph[String, DiEdge]
                             )

trait Module {
  self: EnkiTypes with enki.spark.Analyzers =>

  sealed trait ActionTree[A]

  final case class ActionLeaf[A](stage: Stage[Unit]) extends ActionTree[A]

  final case class ActionNode[A](actions: Map[String, A]) extends ActionTree[A]

  def buildActionTree(
                       rootName: String,
                       p: ProgramS[Stage[Unit]]
                     )
                     (
                       implicit programSplitter: FSHandler[ProgramOp, StageWriter[StageOp, ?]]
                     ): Fix[ActionTree] = {
    val (stages, lastStage) = p.interpret[StageWriter[StageOp, ?]].run
    val actions = ((rootName, lastStage) :: stages)
      .collect { case (name, stage: Stage[Unit]) if stageNonEmpty(stage) => name -> Fix(ActionLeaf[Fix[ActionTree]](stage)) }
      .toMap
    Fix(ActionNode(actions))
  }

  def calcDependencies() = {
    //TODO: calc dependencies as unfold

    val alg : Algebra[ActionTree, (ActionDependencies, Fix[ActionTree])] = Algebra {
      case ActionLeaf(stage) => ???
      case ActionNode(actions) => ???
    }

    // val aaa = scheme.gcata(alg.gather(Gather.histo))
  }


}
