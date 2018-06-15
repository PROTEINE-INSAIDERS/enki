package enki
package sources

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

trait Source {
  def qualifiedName(name: Symbol): Symbol

  def read(name: Symbol, session: SparkSession): DataFrame

  def decode[T: TypeTag](dataFrame: DataFrame): Dataset[T]
}
