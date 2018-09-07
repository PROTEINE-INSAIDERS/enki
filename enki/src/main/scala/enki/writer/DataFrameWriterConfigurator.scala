package enki.writer

import cats.data._
import enki._
import enki.writer.DataFrameWriterSettings._
import org.apache.spark.sql.{DataFrameWriter => _, _}

class DataFrameWriterConfigurator[T] extends DataFrameWriter.Handler[State[DataFrameWriterSettings[T], ?]] {
  override protected[this] def mode(saveMode: SaveMode): State[DataFrameWriterSettings[T], Unit] =
    State.modify((dataFrameWriterLens[T] ~ overwrite[T]).modify(_) { case (writer, overwrite) =>
      (
        writer.mode(saveMode),
        if (saveMode == SaveMode.Overwrite) true else false
      )
    })

  override protected[this] def format(source: String): State[DataFrameWriterSettings[T], Unit] =
    State.modify(dataFrameWriterLens.modify(_)(_.format(source)))

  override protected[this] def partitionBy(colNames: Seq[String]): State[DataFrameWriterSettings[T], Unit] =
    State.modify(dataFrameWriterLens.modify(_)(_.partitionBy(colNames: _*)))

  override protected[this] def partition(partitions: Map[String, String]): State[DataFrameWriterSettings[T], Unit] =
    State.modify(partitionLens.set(_)(Some(partitions)))
}
