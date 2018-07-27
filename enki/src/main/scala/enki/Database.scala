package enki

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.TypeTag

trait Database {
  def schema: String

  protected def saveMode: Option[SaveMode] = None

  protected def qualifiedTableName(tableName: String): String = {
    s"$schema.$tableName"
  }

  def readTable(session: SparkSession, tableName: String): DataFrame = {
    session.table(qualifiedTableName(tableName))
  }

  def writeTable(tableName: String, data: DataFrame): Unit = {
    val writer = data.write

    saveMode.foreach(writer.mode)
    writer.saveAsTable(qualifiedTableName(tableName))
  }

  /* syntactic sugar */

  final def dataFrame(rows: Seq[Row], schema: StructType): Stage[DataFrame] =
    enki.dataFrame(rows, schema)

  final def dataset[T: TypeTag](data: Seq[T]): Stage[Dataset[T]] =
    enki.dataset(data)

  final def read[T: TypeTag](tableName: String, restricted: Boolean = false): Stage[Dataset[T]] =
    enki.read[T](this, tableName, restricted)

  final def persist[T: TypeTag](tableName: String, stage: Stage[Dataset[T]]): Program[Stage[Dataset[T]]] =
    enki.persist[T](this, tableName, stage)
}
