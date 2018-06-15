package enki
package plan

import enki.configuration.SourceConfigurator
import enki.sources.Source
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

sealed abstract class PlanOp[T]

object SessionOp extends PlanOp[SparkSession]

case class SourceOp[T: TypeTag](name: Symbol, source: Source) extends PlanOp[Dataset[T]] {
  private[enki] val typeTag = implicitly[TypeTag[T]]
}

case class StageOp[T](name: Symbol, plan: Plan[Dataset[T]]) extends PlanOp[Dataset[T]]