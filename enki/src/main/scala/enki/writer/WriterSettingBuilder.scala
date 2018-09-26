package enki
package writer

import cats.data.State
import enki.writer.WriterSettings._
import org.apache.spark.sql.{DataFrameWriter => _, _}

class WriterSettingBuilder[T] extends DataFrameWriter.Handler[State[WriterSettings[T], ?]] {
  override protected[this] def mode(saveMode: SaveMode): State[WriterSettings[T], Unit] =
    State.modify((configureLens[T] ~ overwriteLens[T]).modify(_) { case (configure, _) =>
      (
        configure(_).mode(saveMode),
        if (saveMode == SaveMode.Overwrite) true else false
      )
    })

  override protected[this] def format(source: String): State[WriterSettings[T], Unit] =
    State.modify(configureLens.modify(_)(f => f(_).format(source)))

  override protected[this] def partitionBy(colNames: Seq[String]): State[WriterSettings[T], Unit] =
    State.modify(configureLens.modify(_)(f => f(_).partitionBy(colNames: _*)))

  override protected[this] def partition(partitions: Seq[(String, String)]): State[WriterSettings[T], Unit] =
    State.modify(partitionLens.modify(_)(p => p ++ partitions))
}
