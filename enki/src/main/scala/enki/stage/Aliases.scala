package enki
package stage

trait Aliases {
  type Stage[F[_]] = stage.StageAlg[F]
  val Stage: stage.StageAlg.type = stage.StageAlg

  type StageCompiler = stage.StageCompiler
  
  /*
  type StageAction[T] = stage.StageAction[T]

  type DataFrameAction = stage.DataFrameAction
  val DataFrameAction: stage.DataFrameAction.type = stage.DataFrameAction

  type DatasetAction[T] = stage.DatasetAction[T]
  val DatasetAction: stage.DatasetAction.type = stage.DatasetAction

  type TableAction = stage.TableAction

  type ReadTableAction = stage.ReadTableAction

  type ReadDataFrameAction = stage.ReadDataFrameAction
  val ReadDataFrameAction: stage.ReadDataFrameAction.type = stage.ReadDataFrameAction

  type ReadDatasetAction[T] = stage.ReadDatasetAction[T]
  val ReadDatasetAction: stage.ReadDatasetAction.type = stage.ReadDatasetAction

  type WriteTableAction = stage.WriteTableAction

  type WriteDataFrameAction = stage.WriteDataFrameAction
  val WriteDataFrameAction: stage.WriteDataFrameAction.type = stage.WriteDataFrameAction

  type WriteDatasetAction[T] = stage.WriteDatasetAction[T]
  val WriteDatasetAction: stage.WriteDatasetAction.type = stage.WriteDatasetAction

  type ArgumentAction = stage.ArgumentAction

  type StringArgumentAction = stage.StringArgumentAction
  val StringArgumentAction: stage.StringArgumentAction.type = stage.StringArgumentAction

  type IntegerArgumentAction = stage.IntegerArgumentAction
  val IntegerArgumentAction: stage.IntegerArgumentAction.type = stage.IntegerArgumentAction
  */
}
