package enki
package stage

import freestyle.free._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@free trait StageAlg {
  def dataFrame(rows: Seq[Row], schema: StructType): FS[DataFrame]

  def dataset[T](data: Seq[T], encoder: Encoder[T]): FS[Dataset[T]]

  def readDataFrame(schemaName: String, tableName: String): FS[DataFrame]

  def readDataset[T](schemaName: String, tableName: String, encoder: Encoder[T], strict: Boolean): FS[Dataset[T]]

  def writeDataFrame(schemaName: String, tableName: String): FS[WriterSettings => DataFrame => Unit]

  def writeDataset[T](schemaName: String, tableName: String, encoder: Encoder[T], strict: Boolean): FS[WriterSettings => Dataset[T] => Unit]
}