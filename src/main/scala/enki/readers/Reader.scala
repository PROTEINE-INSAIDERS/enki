package enki
package readers

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

trait Reader {
  //TODO: это поле используется только при мэппинге ридеров, это очень некрасиво, нужно придумать альтернативный способ.
  def name: Symbol

  def read[T: TypeTag](table: Symbol, session: SparkSession): Dataset[T]
}
