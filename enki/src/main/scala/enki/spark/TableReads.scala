package enki.spark

import cats._
import cats.data._
import enki.spark.plan.PlanAnalyzer
import freestyle.free._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class TableReads[M: Monoid](f: ReadTableAction => M)
                                (implicit planAnalyzer: PlanAnalyzer) extends FSHandler[SparkAlg.Op, Const[M, ?]] {
  private def planReads[A](plan: LogicalPlan): M = {
    Monoid.combineAll(
      planAnalyzer.tableReads(plan)
        .map(ti => f(ReadDataFrameAction(ti.database.getOrElse(""), ti.table))))
  }

  override def apply[A](fa: SparkAlg.Op[A]): Const[M, A] = Const(fa match {
    case SparkAlg.PlanOp(plan) => planReads(plan)

    case SparkAlg.ReadDataFrameOp(schemaName, tableName) => f(ReadDataFrameAction(schemaName, tableName))

    case SparkAlg.ReadDatasetOp(schemaName, tableName, encoder, strict) =>
      f(ReadDatasetAction(schemaName, tableName, encoder, strict))

    case SparkAlg.SqlOp(sqlText) => planReads(planAnalyzer.parsePlan(sqlText))

    case _ => Monoid.empty[M]
  })
}