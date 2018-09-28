package enki.writer

import enki.writer.WriterSettings.{configureLens, overwriteLens}
import org.apache.spark.sql.SaveMode
import org.apache.spark.{sql => spark}
import shapeless._

case class WriterSettings[T](
                              configure: spark.DataFrameWriter[T] => spark.DataFrameWriter[T],
                              partition: Seq[(String, String)],
                              overwrite: Boolean
                            ) {
  def mode(saveMode: SaveMode): WriterSettings[T] = copy(
    configure = configure(_).mode(saveMode),
    overwrite = if (saveMode == SaveMode.Overwrite) true else false
  )
}

object WriterSettings {
  def apply[T](): WriterSettings[T] =
    WriterSettings(configure = identity, partition = Seq.empty, overwrite = false)

  //TODO: линзы не нужны.
  def configureLens[T]: Lens[WriterSettings[T], spark.DataFrameWriter[T] => spark.DataFrameWriter[T]] = lens[WriterSettings[T]] >> 'configure

  def partitionLens[T]: Lens[WriterSettings[T], Seq[(String, String)]] = lens[WriterSettings[T]] >> 'partition

  def overwriteLens[T]: Lens[WriterSettings[T], Boolean] = lens[WriterSettings[T]] >> 'overwrite
}