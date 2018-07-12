package enki

import org.apache.spark.sql._
import scala.reflect.runtime.universe.TypeTag

abstract class Database {
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

  final def read[T: TypeTag](tableName: String): Stage[Dataset[T]] =
    enki.read[T](this, tableName)

  final def persist[T: TypeTag](tableName: String, stage: Stage[Dataset[T]]): Program[Stage[Dataset[T]]] =
    enki.persist[T](this, tableName, stage)
}
