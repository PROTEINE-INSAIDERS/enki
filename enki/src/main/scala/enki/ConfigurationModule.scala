package enki

import cats._

trait ConfigurationModule {
  def tableNameMapper(f: (String, String) => (String, String)): StageAction ~> StageAction = λ[StageAction ~> StageAction] {
    case action: ReadDataFrameAction =>
      val (schemaName, tableName) = f(action.schemaName, action.tableName)
      action.copy(schemaName = schemaName, tableName = tableName)
    case action: ReadDatasetAction[_] =>
      val (schemaName, tableName) = f(action.schemaName, action.tableName)
      action.copy(schemaName = schemaName, tableName = tableName)
    case action: WriteDataFrameAction =>
      val (schemaName, tableName) = f(action.schemaName, action.tableName)
      action.copy(schemaName = schemaName, tableName = tableName)
    case action: WriteDatasetAction[_] =>
      val (schemaName, tableName) = f(action.schemaName, action.tableName)
      action.copy(schemaName = schemaName, tableName = tableName)
    case other => other
  }
}
