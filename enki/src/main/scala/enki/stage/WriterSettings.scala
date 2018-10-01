package enki.stage

import org.apache.spark.sql.SaveMode
import org.apache.spark.{sql => spark}
import shapeless._

//TODO: move to stage package.
case class WriterSettings(
                              configure: spark.DataFrameWriter[_] => spark.DataFrameWriter[_],
                              partition: Seq[(String, String)],
                              overwrite: Boolean
                            ) {
  def setMode(saveMode: SaveMode): WriterSettings = copy(
    configure = configure(_).mode(saveMode),
    overwrite = if (saveMode == SaveMode.Overwrite) true else false
  )

  def addPartition(name: String, value: String): WriterSettings = {
    copy(partition = partition :+ (name -> value))
  }
}

object WriterSettings {
  def apply(): WriterSettings = WriterSettings(identity _, Seq.empty, overwrite = false)

  //TODO: линзы не нужны.
  def configureLens: Lens[WriterSettings, spark.DataFrameWriter[_] => spark.DataFrameWriter[_]] = lens[WriterSettings] >> 'configure

  def partitionLens: Lens[WriterSettings, Seq[(String, String)]] = lens[WriterSettings] >> 'partition

  def overwriteLens: Lens[WriterSettings, Boolean] = lens[WriterSettings] >> 'overwrite
}