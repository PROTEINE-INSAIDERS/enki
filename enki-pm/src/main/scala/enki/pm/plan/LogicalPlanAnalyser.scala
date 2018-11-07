package enki.pm.plan

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import qq.droste.Coalgebra

object LogicalPlanAnalyser {
  private def wrap: Coalgebra[LogicalPlanF, LogicalPlan] = Coalgebra[LogicalPlanF, LogicalPlan] {
    plan => LogicalPlanF(plan, plan.children.toList)
  }

  def reads(plan: LogicalPlan): Set[TableIdentifier] = {
    plan.collect {
      case a: UnresolvedRelation => a.tableIdentifier
    }.toSet
  }

  def writes(plan: LogicalPlan): Set[TableIdentifier] = {
    plan.collect {
      case InsertIntoTable(UnresolvedRelation(ti), _, _, _, _) => ti
    }
  }.toSet
}
