package enki.spark

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.{sql => spark}

case class WriterSettings(
                           configure: spark.DataFrameWriter[_] => spark.DataFrameWriter[_],
                           partition: Seq[(String, String)],
                           tableIdentifier: TableIdentifier,
                           overwrite: Boolean
                         ) {
  def setMode(saveMode: SaveMode): WriterSettings = copy(
    configure = configure(_).mode(saveMode),
    overwrite = if (saveMode == SaveMode.Overwrite) true else false
  )

  def setPartition(partition: (String, String)*): WriterSettings = copy(partition = partition)

  def setPath(path: String): WriterSettings = copy(configure = configure(_).option("path", path))
}

object WriterSettings {
  def apply(tableIdentifier: TableIdentifier): WriterSettings = WriterSettings(
    configure = identity _,
    partition = Seq.empty,
    overwrite = false,
    tableIdentifier = tableIdentifier
  )
}