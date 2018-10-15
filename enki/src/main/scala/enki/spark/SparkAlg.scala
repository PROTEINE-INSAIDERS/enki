package enki
package spark

import freestyle.free._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@free trait SparkAlg {
  def dataFrame(rows: Seq[Row], schema: StructType): FS[DataFrame]

  def dataset[T](data: Seq[T], encoder: Encoder[T]): FS[Dataset[T]]

  def readDataFrame(schemaName: String, tableName: String): FS[ReaderSettings => DataFrame]

  def readDataset[T](schemaName: String, tableName: String, encoder: Encoder[T], strict: Boolean): FS[ReaderSettings => Dataset[T]]

  //TODO: Команда sql может содержать обращения к различным таблицам, и это можно увидеть только на уровне плана команды.
  // для решения задач меппинга таблиц и партиционирования необходимо уметь трансформировать этот план.
  //TODO: принимать трансформатор плана аппликативным образом. 
  def sql(sqlText: String): FS[DataFrame]

  def writeDataFrame(schemaName: String, tableName: String): FS[WriterSettings => DataFrame => Unit]

  def writeDataset[T](schemaName: String, tableName: String, encoder: Encoder[T], strict: Boolean): FS[WriterSettings => Dataset[T] => Unit]
}