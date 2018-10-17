package enki.spark

import org.apache.spark.sql._
import org.apache.spark.{sql => spark}

case class WriterSettings(
                           configure: spark.DataFrameWriter[_] => spark.DataFrameWriter[_],
                           partition: Seq[(String, String)],
                           overwrite: Boolean
                         ) {
  def setMode(saveMode: SaveMode): WriterSettings = copy(
    configure = configure(_).mode(saveMode),
    overwrite = if (saveMode == SaveMode.Overwrite) true else false
  )


  def setPartition(partition: (String, String)*): WriterSettings = copy(partition = partition)
}

object WriterSettings {
  def apply(): WriterSettings = WriterSettings(identity _, Seq.empty, overwrite = false)
}