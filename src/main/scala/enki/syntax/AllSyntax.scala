package enki
package syntax

import enki.builder.{ExecutionPlanBuilder, Source}
import enki.devonly.Result

trait AllSyntax extends TestSyntax {
  def source(id: Symbol): ExecutionPlanBuilder[Result] = Source(id)

  def stage(id: Symbol)(table: ExecutionPlanBuilder[Result]): ExecutionPlanBuilder[Result] = ???
}
