package enki.writer

import cats.data._
import enki.writer.DataFrameWriterSettings._
import org.apache.spark.sql.{DataFrameWriter => _, _}

class DataFrameWriterSettingHandler[T] extends DataFrameWriterBase.Handler[State[DataFrameWriterSettings[T], ?]] {
  override protected[this] def mode(saveMode: SaveMode): State[DataFrameWriterSettings[T], Unit] =
    State.modify((configureLens[T] ~ overwriteLens[T]).modify(_) { case (configure, _) =>
      (
        configure(_).mode(saveMode),
        if (saveMode == SaveMode.Overwrite) true else false
      )
    })

  override protected[this] def format(source: String): State[DataFrameWriterSettings[T], Unit] =
    State.modify(configureLens.modify(_)(f => f(_).format(source)))

  override protected[this] def partitionBy(colNames: Seq[String]): State[DataFrameWriterSettings[T], Unit] =
    State.modify(configureLens.modify(_)(f => f(_).partitionBy(colNames: _*)))

  override protected[this] def partition(partitions: Seq[(String, String)]): State[DataFrameWriterSettings[T], Unit] =
    State.modify(partitionLens.modify(_)(p => p ++ partitions))
}
