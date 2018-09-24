package enki.writer

import org.apache.spark.{sql => spark}
import shapeless._

case class DataFrameWriterSettings[T](
                                       dataFrameWriter: spark.DataFrameWriter[T],
                                       partition: Option[Map[String, String]], //TODO: убрать Option? (пустой Map эквивалентен отсутсвию партиционирования).
                                       overwrite: Boolean
                                     )

object DataFrameWriterSettings {
  def apply[T](dataFrameWriter: spark.DataFrameWriter[T]): DataFrameWriterSettings[T] =
    DataFrameWriterSettings(dataFrameWriter, Some(Map.empty), overwrite = false)

  def dataFrameWriterLens[T]: Lens[DataFrameWriterSettings[T], spark.DataFrameWriter[T]] = lens[DataFrameWriterSettings[T]] >> 'dataFrameWriter

  def partitionLens[T]: Lens[DataFrameWriterSettings[T], Option[Map[String, String]]] = lens[DataFrameWriterSettings[T]] >> 'partition

  def overwrite[T]: Lens[DataFrameWriterSettings[T], Boolean] = lens[DataFrameWriterSettings[T]] >> 'overwrite
}