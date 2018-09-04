package enki.writer

import freestyle.free._
import org.apache.spark.sql._

@free trait DataFrameWriter {
  def mode(saveMode: SaveMode): FS[Unit]

  def format(source: String): FS[Unit]

  def partitionBy(colNames: Seq[String]): FS[Unit]
}
