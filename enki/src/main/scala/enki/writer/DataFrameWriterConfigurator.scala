package enki.writer

import cats.data._
import enki._
import org.apache.spark.sql.{DataFrameWriter => _, _}
import org.apache.spark.{sql => spark}

class DataFrameWriterConfigurator[T] extends DataFrameWriter.Handler[DataFrameWriterState[T, ?]] {
  override protected[this] def mode(saveMode: SaveMode): State[spark.DataFrameWriter[T], Unit] =
    State.modify(_.mode(saveMode))

  override protected[this] def format(source: String): State[spark.DataFrameWriter[T], Unit] =
    State.modify(_.format(source))

  override protected[this] def partitionBy(colNames: Seq[String]): State[spark.DataFrameWriter[T], Unit] =
    State.modify(_.partitionBy(colNames: _*))
}
