package enki

import cats._
import cats.free.FreeApplicative
import cats.free.FreeApplicative._
import cats.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

trait StageModule {

  sealed trait StageAction[T]

  final case class DataFrameAction(rows: Seq[Row], schema: StructType) extends StageAction[DataFrame]

  final case class DatasetAction[T](data: Seq[T], encoder: Encoder[T]) extends StageAction[Dataset[T]]

  trait TableAction {
    def schemaName: String

    def tableName: String

    override def toString: String = s"$schemaName.$tableName"
  }

  trait ReadTableAction extends TableAction

  //TODO: ReadAction и WriteAction могут содержать много настроечных параметров. Необходимо вынести их в ReaderSettings и
  // WriterSettings, чтобы в дальнейшем уменьнить количество рефакторингов при добавлении новых параметров.
  final case class ReadDataFrameAction(
                                        schemaName: String,
                                        tableName: String
                                      ) extends StageAction[DataFrame] with ReadTableAction

  final case class ReadDatasetAction[T](
                                         schemaName: String,
                                         tableName: String,
                                         encoder: Encoder[T],
                                         strict: Boolean
                                       ) extends StageAction[Dataset[T]] with ReadTableAction

  trait WriteTableAction extends TableAction

  final case class WriteDataFrameAction(
                                         schemaName: String,
                                         tableName: String,
                                         saveMode: Option[SaveMode]
                                       ) extends StageAction[DataFrame => Unit] with WriteTableAction

  final case class WriteDatasetAction[T](
                                          schemaName: String,
                                          tableName: String,
                                          encoder: Encoder[T],
                                          strict: Boolean,
                                          saveMode: Option[SaveMode]
                                        ) extends StageAction[Dataset[T] => Unit] with WriteTableAction

  type Stage[A] = FreeApplicative[StageAction, A]

  def dataFrame(rows: Seq[Row], schema: StructType): Stage[DataFrame] =
    lift[StageAction, DataFrame](DataFrameAction(rows, schema))

  def dataset[T](data: Seq[T], encoder: Encoder[T]): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](DatasetAction(data, encoder))

  def emptyStage: Stage[Unit] = pure(Unit)

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
                      saveMode: Option[SaveMode]
                    ): Stage[DataFrame => Unit] =
    lift[StageAction, DataFrame => Unit](WriteDataFrameAction(schemaName, tableName, saveMode))

  def writeDataset[T](
                       schemaName: String,
                       tableName: String,
                       encoder: Encoder[T],
                       strict: Boolean,
                       saveMode: Option[SaveMode]
                     ): Stage[Dataset[T] => Unit] =
    lift[StageAction, Dataset[T] => Unit](WriteDatasetAction(schemaName, tableName, encoder, strict, saveMode))

  def stageCompiler: StageAction ~> SparkAction = λ[StageAction ~> SparkAction] {
    case action: DataFrameAction => session: SparkSession =>
      session.createDataFrame(action.rows, action.schema)

    case action: DatasetAction[t] => session: SparkSession =>
      session.createDataset[t](action.data)(action.encoder)

    case action: ReadDataFrameAction => session: SparkSession =>
      session.table(s"${action.schemaName}.${action.tableName}")

    case action: ReadDatasetAction[t] => session: SparkSession => {
      val dataframe = session.table(s"${action.schemaName}.${action.tableName}")
      val restricted = if (action.strict) {
        dataframe.select(action.encoder.schema.map(f => dataframe(f.name)): _*)
      } else {
        dataframe
      }
      restricted.as[t](action.encoder)
    }

    case action: WriteDataFrameAction => _: SparkSession =>
      dataFrame: DataFrame => {
        val writer = dataFrame.write
        action.saveMode.foreach(writer.mode)
        writer.saveAsTable(s"${action.schemaName}.${action.tableName}")
      }

    case action: WriteDatasetAction[t] => _: SparkSession =>
      dataset: Dataset[t] => {
        val resticted = if (action.strict) {
          dataset.select(action.encoder.schema.map(f => dataset(f.name)): _*)
        } else {
          dataset
        }
        val writer = resticted.as[t](action.encoder).write //TODO: возможно в некоторых случаях этот каст лишний.
        action.saveMode.foreach(writer.mode)
        writer.saveAsTable(s"${action.schemaName}.${action.tableName}")
      }
  }

  def stageReads(stage: Stage[_]): Set[ReadTableAction] = {
    stage.analyze(λ[StageAction ~> λ[α => Set[ReadTableAction]]] {
      case r: ReadTableAction => Set(r)
      case _ => Set.empty
    })
  }

  def stageWrites(stage: Stage[_]): Set[WriteTableAction] = {
    stage.analyze(λ[StageAction ~> λ[α => Set[WriteTableAction]]] {
      case w: WriteTableAction => Set(w)
      case _ => Set.empty
    })
  }

  def stageNonEmpty(stage: Stage[_]): Boolean = {
    stage.analyze(λ[StageAction ~> λ[α => Option[Unit]]] {
      case _ => Some(Unit)
    }).nonEmpty
  }
}