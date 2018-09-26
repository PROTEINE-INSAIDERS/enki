package enki.writer

import org.apache.spark.{sql => spark}
import shapeless._

case class DataFrameWriterSettings[T](
                                       configure: spark.DataFrameWriter[T] => spark.DataFrameWriter[T],
                                       partition: Seq[(String, String)],
                                       overwrite: Boolean
                                     )

object DataFrameWriterSettings {
  def apply[T](): DataFrameWriterSettings[T] =
    DataFrameWriterSettings(configure = identity, partition = Seq.empty, overwrite = false)

  def configureLens[T]: Lens[DataFrameWriterSettings[T], spark.DataFrameWriter[T] => spark.DataFrameWriter[T]] = lens[DataFrameWriterSettings[T]] >> 'configure

  def partitionLens[T]: Lens[DataFrameWriterSettings[T], Seq[(String, String)]] = lens[DataFrameWriterSettings[T]] >> 'partition

  def overwriteLens[T]: Lens[DataFrameWriterSettings[T], Boolean] = lens[DataFrameWriterSettings[T]] >> 'overwrite
}