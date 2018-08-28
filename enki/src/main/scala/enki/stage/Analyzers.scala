package enki.stage

import cats._
import cats.implicits._
import enki._

trait Analyzers {
  def stageArguments[M: Monoid](stage: Stage[_], f: ArgumentAction => M): M = {
    stage.analyze(λ[StageAction ~> λ[α => M]] {
      case r: ArgumentAction => f(r)
      case _ => implicitly[Monoid[M]].empty
    })
  }

  def stageReads[M: Monoid](stage: Stage[_], f: ReadTableAction => M): M = {
    stage.analyze(λ[StageAction ~> λ[α => M]] {
      case r: ReadTableAction => f(r)
      case _ => implicitly[Monoid[M]].empty
    })
  }

  def stageWrites[M: Monoid](stage: Stage[_], f: WriteTableAction => M): M = {
    stage.analyze(λ[StageAction ~> λ[α => M]] {
      case w: WriteTableAction => f(w)
      case _ => implicitly[Monoid[M]].empty
    })
  }

  def stageNonEmpty(stage: Stage[_]): Boolean = {
    stage.analyze(λ[StageAction ~> λ[α => Option[Unit]]] {
      case _ => Some(())
    }).nonEmpty
  }
}
