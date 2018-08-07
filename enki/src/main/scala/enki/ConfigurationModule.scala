package enki

import cats._

trait ConfigurationModule {
  //TODO: следует ли добавить информацию о том, читается данная таблица, записывается, или оба варианта.
  def tableNameMapper(f: (String, String) => (String, String)): StageAction ~> StageAction = λ[StageAction ~> StageAction] {
    case read: ReadAction[t] =>
      val (schemaName, tableName) = f(read.schemaName, read.tableName)
      read.copy[t](schemaName = schemaName, tableName = tableName)(read.tag)
    case write: WriteAction[t] =>
      val (schemaName, tableName) = f(write.schemaName, write.tableName)
      write.copy[t](schemaName = schemaName, tableName = tableName)
    case other => other
  }
}
