package enki
package writers

import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

trait TableWriter extends Writer {
  protected def getTableName(table: Symbol): String

  protected def encode[T: TypeTag](data: Dataset[T]): DataFrame = data.toDF()

  override def write[T: TypeTag](table: Symbol, data: Dataset[T], session: SparkSession): Unit = {
    data.write.saveAsTable(getTableName(table))
  }
}
