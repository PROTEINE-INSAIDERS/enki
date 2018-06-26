package enki
package readers

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

trait Reader {
  def read[T: TypeTag](table: Symbol, session: SparkSession): Dataset[T]
}
