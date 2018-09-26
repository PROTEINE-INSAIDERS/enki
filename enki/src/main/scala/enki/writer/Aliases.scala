package enki.writer

import cats.data.State
import enki._
import org.apache.spark.{sql => spark}

trait Aliases {
  type DataFrameWriterState[T, A] = State[spark.DataFrameWriter[T], A]

  type DataFrameWriterSettingHandler[T] = writer.DataFrameWriterSettingHandler[T]
  type DataFrameWriter[F[_]] = writer.DataFrameWriterBase[F]
  val DataFrameWriter: writer.DataFrameWriterBase.type = writer.DataFrameWriterBase
  type DataFrameWriterSettings[T] = writer.DataFrameWriterSettings[T]
  val DataFrameWriterSettings: writer.DataFrameWriterSettings.type = writer.DataFrameWriterSettings
}
