package enki
package stage

import cats._
import cats.implicits._

trait Analyzers {
  self: Enki =>

  //TODO: перенести в пакет к аргументам.
  def stageArguments[M: Monoid](
                                 stage: Stage[_],
                                 f: ArgumentAction => M
                               ): M = {
    analyzeArgs(stage, λ[ArgsAlg.Op ~> λ[α => M]] {
      case ArgsAlg.StringOp(name, description, defaultValue) => f(StringArgumentAction(name, description, defaultValue))
      case ArgsAlg.IntOp(name, description, defaultValue) => f(IntegerArgumentAction(name, description, defaultValue))
      case ArgsAlg.BoolOp(name, description, defaultValue) => f(BooleanArgumentAction(name, description, defaultValue))
    })
  }

  def stageReads[M: Monoid](
                             stage: Stage[_],
                             f: ReadTableAction => M
                           ): M = {
    analyzeStages(stage, λ[StageAlg.Op ~> λ[α => M]] {
      case StageAlg.ReadDataFrameOp(schemaName, tableName) => f(ReadDataFrameAction(schemaName, tableName))
      case StageAlg.ReadDatasetOp(schemaName, tableName, encoder, strict) => f(ReadDatasetAction(schemaName, tableName, encoder, strict))
      case _ => Monoid.empty[M]
    })
  }

  def stageWrites[M: Monoid](
                              stage: Stage[_],
                              f: WriteTableAction => M
                            ): M = {
    analyzeStages(stage, λ[StageAlg.Op ~> λ[α => M]] {
      case StageAlg.WriteDataFrameOp(schemaName, tableName) => f(WriteDataFrameAction(schemaName, tableName))
      case StageAlg.WriteDatasetOp(schemaName, tableName, encoder, strict) => f(WriteDatasetAction(schemaName, tableName, encoder, strict))
      case _ => Monoid.empty[M]
    })
  }

  //TODO: implement analysers atop of compilers.
  def stageNonEmpty(stage: Stage[_]): Boolean = {
    stage.analyze(λ[StageOp ~> λ[α => Option[Unit]]] {
      case _ => Some(())
    }).nonEmpty
  }
}
