package enki.spark

import enki.spark.SparkAlg._
import freestyle.free._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal._

/**
  * Map table names.
  */
case class TableMapper(f: TableIdentifier => TableIdentifier)(implicit val session: SparkSession) extends FSHandler[Op, Op] {
  private def mapPlan(logicalPlan: LogicalPlan): LogicalPlan = {
    logicalPlan.transform {
      case UnresolvedRelation(identifier) => UnresolvedRelation(f(identifier))
    }
  }

  override def apply[A](fa: Op[A]): Op[A] = fa match {
    case a: ReadDataFrameOp =>
      val id = f(TableIdentifier(a.tableName, Some(a.schemaName)))
      a.copy(schemaName = id.database.getOrElse(""), tableName = id.table)
    case a: ReadDatasetOp[t] =>
      val id = f(TableIdentifier(a.tableName, Some(a.schemaName)))
      a.copy(schemaName = id.database.getOrElse(""), tableName = id.table)
    case a: WriteDataFrameOp =>
      val id = f(TableIdentifier(a.tableName, Some(a.schemaName)))
      a.copy(schemaName = id.database.getOrElse(""), tableName = id.table)
    case a: WriteDatasetOp[t] =>
      val id = f(TableIdentifier(a.tableName, Some(a.schemaName)))
      a.copy(schemaName = id.database.getOrElse(""), tableName = id.table)
    case PlanOp(plan) => PlanOp(mapPlan(plan))
    case SqlOp(sql) => PlanOp(mapPlan(session.sessionState.sqlParser.parsePlan(sql)))
    case other => other
  }
}