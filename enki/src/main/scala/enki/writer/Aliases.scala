package enki.writer

import cats.data.State
import enki._
import org.apache.spark.{sql => spark}

trait Aliases {
  type WriterState[T, A] = State[spark.DataFrameWriter[T], A]

  type WriterSettingBuilder[T] = writer.WriterSettingBuilder[T]

  type DataFrameWriter[F[_]] = writer.DataFrameWriter[F]

  val DataFrameWriter: writer.DataFrameWriter.type = writer.DataFrameWriter
  type WriterSettings[T] = writer.WriterSettings[T]
  val WriterSettings: writer.WriterSettings.type = writer.WriterSettings
}
