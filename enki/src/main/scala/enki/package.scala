import org.apache.spark.sql.catalyst.plans.logical._

/**
  * Enki static type aliases
  */
package object enki
  extends AllModules
    with spark.Module
    // with stage.Syntax
    with program.Module {

  type PlanTransformer = LogicalPlan => LogicalPlan
}