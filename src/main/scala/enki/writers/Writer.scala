package enki.writers

import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

trait Writer {
  def write[T: TypeTag](name: Symbol, data: Dataset[T], session: SparkSession): Unit
}
