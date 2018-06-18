package enki
package program

import enki.sources.Source
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

sealed abstract class Statement[T]

object SessionSt extends Statement[SparkSession]

case class SourceSt[T: TypeTag](name: Symbol, source: Source) extends Statement[Dataset[T]] {
  private[enki] val typeTag = implicitly[TypeTag[T]]
}

case class StageSt[T](name: Symbol, program: Program[Dataset[T]]) extends Statement[Dataset[T]]