package enki
package instances

import cats.Applicative
import enki.builder.{ExecutionPlanBuilder, Fake}

trait ExecutionPlanBuilderInstances {
  implicit val enkiInstancesForExecutionPlanBuilder: Applicative[ExecutionPlanBuilder] = new Applicative[ExecutionPlanBuilder] {
    override def pure[A](x: A): ExecutionPlanBuilder[A] = Fake[A]()

    override def ap[A, B](ff: ExecutionPlanBuilder[A => B])(fa: ExecutionPlanBuilder[A]): ExecutionPlanBuilder[B] = Fake[B]()
  }
}
