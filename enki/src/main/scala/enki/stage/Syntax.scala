package enki.stage

import cats.free.FreeApplicative
import cats.free.FreeApplicative._
import enki._
import enki.writer.DataFrameWriterBase
import freestyle.free.FreeS
import org.apache.spark.sql._
import org.apache.spark.sql.types._

trait Syntax {
  type Stage[A] = FreeApplicative[StageAction, A]

  def dataFrame(rows: Seq[Row], schema: StructType): Stage[DataFrame] =
    lift[StageAction, DataFrame](DataFrameAction(rows, schema))

  def dataset[T](data: Seq[T], encoder: Encoder[T]): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](DatasetAction(data, encoder))

  def emptyStage: Stage[Unit] = pure(())

  def readDataFrame(
                     schemaName: String,
                     tableName: String
                   ): Stage[DataFrame] =
    lift[StageAction, DataFrame](ReadDataFrameAction(schemaName, tableName))

  def readDataset[T](
                      schemaName: String,
                      tableName: String,
                      encoder: Encoder[T],
                      strict: Boolean
                    ): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](ReadDatasetAction(schemaName, tableName, encoder, strict))

  def writeDataFrame(
                      schemaName: String,
                      tableName: String,
                      writerSettings: FreeS.Par[DataFrameWriter.Op, Unit]
                    ): Stage[DataFrame => Unit] =
    lift[StageAction, DataFrame => Unit](WriteDataFrameAction(schemaName, tableName, writerSettings))

  def writeDataset[T](
                       schemaName: String,
                       tableName: String,
                       encoder: Encoder[T],
                       strict: Boolean,
                       writerSettings: FreeS.Par[DataFrameWriter.Op, Unit]
                     ): Stage[Dataset[T] => Unit] =
    lift[StageAction, Dataset[T] => Unit](WriteDatasetAction(schemaName, tableName, encoder, strict, writerSettings))

  def integerArgument(name: String, description: String, defaultValue: Option[Int]): Stage[Int] =
    lift[StageAction, Int](IntegerArgumentAction(name, description, defaultValue))

  def stringArgument(name: String, description: String, defaultValue: Option[String]): Stage[String] =
    lift[StageAction, String](StringArgumentAction(name, description, defaultValue))
}
