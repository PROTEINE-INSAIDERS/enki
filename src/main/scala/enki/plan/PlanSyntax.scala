package enki
package plan

import cats.free.FreeApplicative._
import cats.free._
import enki.configuration.SourceConfiguration
import org.apache.spark.sql._
import scala.reflect.runtime.universe.TypeTag

trait PlanSyntax {
  type Plan[A] = FreeApplicative[PlanOp, A]

  def source[T: TypeTag](name: Symbol)(implicit config: SourceConfiguration): Plan[Dataset[T]] =
    lift[PlanOp, Dataset[T]](Source[T](name, config))

  def session: Plan[SparkSession] = lift(Session)
}
