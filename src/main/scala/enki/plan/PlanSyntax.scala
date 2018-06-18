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

  //TODO: попробовать Plan[Dataset[T] => Dataset[T]]
  def stage[T](name: Symbol, plan: Plan[Dataset[T]]): Plan[Dataset[T]] = lift[PlanOp, Dataset[T]](StageOp(name, plan))
}
