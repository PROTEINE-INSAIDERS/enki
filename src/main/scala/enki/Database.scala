package enki

import org.apache.spark.sql._

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
}
