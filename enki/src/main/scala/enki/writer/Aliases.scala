package enki.writer

import cats.data.State
import enki._
import org.apache.spark.{sql => spark}

trait Aliases {
  type DataFrameWriterState[T, A] = State[spark.DataFrameWriter[T], A]

  type DataFrameWriterConfigurator[T] = writer.DataFrameWriterConfigurator[T]
  type DataFrameWriter[F[_]] = writer.DataFrameWriter[F]
  val DataFrameWriter: writer.DataFrameWriter.type = writer.DataFrameWriter
}
