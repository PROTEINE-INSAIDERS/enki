package enki
package stage

trait Aliases {
  type StageAlg[F[_]] = stage.StageAlg[F]
  val StageAlg: stage.StageAlg.type = stage.StageAlg

  type DefaultStageCompiler = stage.DefaultStageCompiler

  type ReadTableAction = stage.ReadTableAction

  type ReadDatasetAction[A] = stage.ReadDatasetAction[A]

  type WriteTableAction = stage.WriteTableAction

  type WriterSettings = stage.WriterSettings
  val WriterSettings: stage.WriterSettings.type = stage.WriterSettings

  type TableNameMapper = stage.TableNameMapper
}
