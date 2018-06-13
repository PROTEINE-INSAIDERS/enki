package enki
package plan

import enki.configuration.SourceConfiguration
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

sealed abstract class PlanOp[T]

object Session extends PlanOp[SparkSession]

case class Source[T: TypeTag](table: Symbol, config: SourceConfiguration) extends PlanOp[Dataset[T]] {
  private[enki] val typeTag = implicitly[TypeTag[T]]

  private[enki] val qualifiedTableName = config.resolveTableName(table)
}

case class Stage[T](name: Symbol, plan: Plan[Dataset[T]]) extends PlanOp[Dataset[T]]