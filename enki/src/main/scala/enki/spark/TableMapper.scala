package enki.spark

import enki.spark.SparkAlg._
import enki.spark.plan.PlanAnalyzer
import freestyle.free._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._

/**
  * Map table names.
  */
case class TableMapper(f: TableIdentifier => TableIdentifier)
                      (implicit planAnalyzer: PlanAnalyzer) extends FSHandler[Op, Op] {
  private def mapPlan(logicalPlan: LogicalPlan): LogicalPlan = {
    logicalPlan.transform {
      case UnresolvedRelation(identifier) => UnresolvedRelation(f(identifier))
    }
  }

  override def apply[A](fa: Op[A]): Op[A] = fa match {
    case a: ReadDataFrameOp =>
      val id = TableIdentifier(a.tableName, Some(a.schemaName))
      a.copy(schemaName = id.database.getOrElse(""), tableName = id.table)
    case a: ReadDatasetOp[t] =>
      val id = TableIdentifier(a.tableName, Some(a.schemaName))
      a.copy(schemaName = id.database.getOrElse(""), tableName = id.table)
    case a: WriteDataFrameOp =>
      val id = TableIdentifier(a.tableName, Some(a.schemaName))
      a.copy(schemaName = id.database.getOrElse(""), tableName = id.table)
    case a: WriteDatasetOp[t] =>
      val id = TableIdentifier(a.tableName, Some(a.schemaName))
      a.copy(schemaName = id.database.getOrElse(""), tableName = id.table)
    case PlanOp(plan) => PlanOp(mapPlan(plan))
    case SqlOp(sql) => PlanOp(mapPlan(planAnalyzer.parsePlan(sql)))
    case other => other
  }
}