package enki.spark.plan

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}

import scala.collection._

trait PlanAnalyzer {
  def tableReads(plan: LogicalPlan): Seq[TableIdentifier] = {
    plan.collect { case (a: UnresolvedRelation) => a.tableIdentifier }
  }

  def mapTableNames(plan: LogicalPlan, f: TableIdentifier => TableIdentifier): LogicalPlan = {
    case class LogicalPlanRefWrapper(logicalPlan: LogicalPlan) {
      override def equals(obj: scala.Any): Boolean = obj match {
        case other: LogicalPlanRefWrapper => this.logicalPlan eq other.logicalPlan
        case _ => super.equals(obj)
      }

      override def hashCode(): Int = logicalPlan.hashCode()
    }

    val excluded = mutable.Set[LogicalPlanRefWrapper]()

    def excludeAll(logicalPlan: LogicalPlan): Unit = {
      logicalPlan.foreach(p => {
        excluded.add(LogicalPlanRefWrapper(p)); ()
      })
    }

    def planExcluded(logicalPlan: LogicalPlan) = excluded(LogicalPlanRefWrapper(logicalPlan))

    plan.transformDown {
      case a@UnresolvedRelation(identifier) if !planExcluded(a) => {
        UnresolvedRelation(f(identifier))
      }
      case a@With(child, cteRelations) if !planExcluded(a) => {
        def excludingTable(bound: Set[String])(ti: TableIdentifier): TableIdentifier = {
          if (ti.database.isEmpty && bound.contains(ti.table)) {
            ti
          } else {
            f(ti)
          }
        }
        val mappedRelations = cteRelations.reverse.tails.toSeq.init.map { expressions =>
          val (alias, subqueryAlias) = expressions.head
          (alias,
            subqueryAlias.copy(child = mapTableNames(subqueryAlias.child, excludingTable(expressions.tail.map(_._1).toSet)))
          )
        }.reverse
        val mappedChild = mapTableNames(child, excludingTable(mappedRelations.map(_._1).toSet))
        excludeAll(mappedChild)
        With(mappedChild, mappedRelations)
      }
    }
  }
}
