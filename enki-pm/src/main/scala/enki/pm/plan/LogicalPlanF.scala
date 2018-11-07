package enki.pm.plan

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import qq.droste.Coalgebra

case class LogicalPlanF[A](node: LogicalPlan, children: List[A])
