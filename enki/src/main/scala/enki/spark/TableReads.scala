package enki.spark

import cats._
import cats.data._
import enki.spark.plan.PlanAnalyzer
import freestyle.free._

class TableReads[M: Monoid](f: ReadTableAction => M)
                           (implicit planAnalyzer: PlanAnalyzer) extends FSHandler[SparkAlg.Op, Const[M, ?]] {
  override def apply[A](fa: SparkAlg.Op[A]): Const[M, A] = Const(fa match {
    case SparkAlg.ReadDataFrameOp(schemaName, tableName) => f(ReadDataFrameAction(schemaName, tableName))

    case SparkAlg.ReadDatasetOp(schemaName, tableName, encoder, strict) =>
      f(ReadDatasetAction(schemaName, tableName, encoder, strict))

    case SparkAlg.SqlOp(sqlText) =>
      Monoid.combineAll(
        planAnalyzer.tableReads(planAnalyzer.parsePlan(sqlText))
          .map(ti => f(ReadDataFrameAction(ti.database.getOrElse(""), ti.table))))

    case _ => Monoid.empty[M]
  })
}
