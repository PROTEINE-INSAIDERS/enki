package enki.spark

import enki.spark.SparkAlg._
import freestyle.free._

class TableNameMapper(f: (String, String) => (String, String)) extends FSHandler[Op, Op] {
  override def apply[A](fa: Op[A]): Op[A] = fa match {
    case a: ReadDataFrameOp =>
      val (schema, table) = f(a.schemaName, a.tableName)
      a.copy(schemaName = schema, tableName = table)
    case a: ReadDatasetOp[t] =>
      val (schema, table) = f(a.schemaName, a.tableName)
      a.copy(schemaName = schema, tableName = table)
    case a: WriteDataFrameOp =>
      val (schema, table) = f(a.schemaName, a.tableName)
      a.copy(schemaName = schema, tableName = table)
    case a: WriteDatasetOp[t] =>
      val (schema, table) = f(a.schemaName, a.tableName)
      a.copy(schemaName = schema, tableName = table)
    case other => other
  }
}

object TableNameMapper {
  def apply(f: (String, String) => (String, String)): TableNameMapper = new TableNameMapper(f)
}