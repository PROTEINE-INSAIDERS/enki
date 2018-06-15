package enki
package plan

import cats.free.FreeApplicative._
import enki.sources.Source
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

trait PlanSyntax {
  def source[T: TypeTag](name: Symbol)(implicit source: Source): Plan[Dataset[T]] =
    lift[PlanOp, Dataset[T]](SourceOp[T](name, source))

  def session: Plan[SparkSession] = lift(SessionOp)

  // это приводит к необходимости использования свободной монады.
  // def stage0[T](name: Symbol, dataset: Dataset[T]): Plan[Dataset[T]] = liftF[PlanOp, Dataset[T]](Stage(name, dataset))

  def stage[T](name: Symbol, plan: Plan[Dataset[T]]): Plan[Dataset[T]] = lift[PlanOp, Dataset[T]](StageOp(name, plan))
}
