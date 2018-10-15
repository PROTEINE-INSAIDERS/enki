package enki.spark.plan

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal._

trait PlanAnalyzer {
  def parser: AbstractSqlParser = new SparkSqlParser(new SQLConf())

  def parsePlan(sqlText: String): LogicalPlan = {
    parser.parsePlan(sqlText)
  }

  def tableReads(plan: LogicalPlan ): Seq[TableIdentifier] = {
    plan.collect { case (a : UnresolvedRelation) => a.tableIdentifier  }
  }
}
