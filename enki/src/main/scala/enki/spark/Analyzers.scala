package enki
package spark

import cats._
import cats.implicits._

trait Analyzers {
  self: Enki =>

  def stageNonEmpty(stage: Stage[_]): Boolean = {
    stage.analyze(λ[StageOp ~> λ[α => Option[Unit]]] {
      case _ => Some(())
    }).nonEmpty
  }
}
