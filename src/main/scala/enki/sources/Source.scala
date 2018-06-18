package enki
package sources

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

trait Source {
  def qualifiedName(name: Symbol): Symbol

  def read[T: TypeTag](name: Symbol, session: SparkSession): Dataset[T]
}
