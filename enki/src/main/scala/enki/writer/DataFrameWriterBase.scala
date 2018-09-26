package enki
package writer

import freestyle.free._
import org.apache.spark.sql._

@free trait DataFrameWriterBase {
  def mode(saveMode: SaveMode): FS[Unit]

  def format(source: String): FS[Unit]

  def partitionBy(colNames: Seq[String]): FS[Unit]

  def partition(partition: Seq[(String, String)]): FS[Unit] //TODO: rename to "static partition" (Spark 2.3 will support dynamic partitioning out of the box).
}

@module trait DataFrameWriter {
  val dataFrameWriter: DataFrameWriterBase
  val arguments: Arguments
}