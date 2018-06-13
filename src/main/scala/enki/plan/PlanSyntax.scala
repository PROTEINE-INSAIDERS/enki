package enki
package plan

import cats.free.FreeApplicative._
import cats.free._
import enki.configuration.SourceConfiguration
import org.apache.spark.sql._
import scala.reflect.runtime.universe.TypeTag

trait PlanSyntax {
  def source[T: TypeTag](name: Symbol)(implicit config: SourceConfiguration): Plan[Dataset[T]] =
    lift[PlanOp, Dataset[T]](Source[T](name, config))

  def session: Plan[SparkSession] = lift(Session)

  // это приводит к необходимости использования свободной монады.
  // def stage0[T](name: Symbol, dataset: Dataset[T]): Plan[Dataset[T]] = liftF[PlanOp, Dataset[T]](Stage(name, dataset))

  def stage[T](name: Symbol, plan: Plan[Dataset[T]]): Plan[Dataset[T]] = lift[PlanOp, Dataset[T]](Stage(name, plan))
}
