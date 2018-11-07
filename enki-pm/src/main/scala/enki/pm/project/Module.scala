package enki.pm.project

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

sealed trait Module {
  def qualifiedName: ModuleQName
}

final case class SqlModule(
                            qualifiedName: ModuleQName,
                            sql: String,
                            plan: LogicalPlan,
                            arguments: Set[String],
                            reads: Set[TableIdentifier],
                            writes: Set[TableIdentifier]
                          ) extends Module

