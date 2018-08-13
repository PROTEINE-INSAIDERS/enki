package enki

import cats._
import cats.free.FreeApplicative
import cats.free.FreeApplicative._
import cats.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

trait StageModule {

  sealed trait StageAction[T]

  //TODO: ReadAction и WriteAction могут содержать много настроечных параметров. Необходимо вынести их в ReaderSettings и
  // WriterSettings, чтобы в дальнейшем уменьнить количество рефакторингов при добавлении новых параметров.
  final case class ReadAction[T: TypeTag](
                                           schemaName: String,
                                           tableName: String,
                                           strict: Boolean
                                         ) extends StageAction[Dataset[T]] {
    def tag: TypeTag[T] = implicitly
  }

  final case class WriteAction[T: TypeTag](
                                            schemaName: String,
                                            tableName: String,
                                            strict: Boolean,
                                            allowTruncate: Boolean,
                                            saveMode: Option[SaveMode]
                                          ) extends StageAction[Dataset[T] => Unit] {
    def tag: TypeTag[T] = implicitly
  }

  final case class DatasetAction[T: TypeTag](data: Seq[T], strict: Boolean, allowTruncate: Boolean) extends StageAction[Dataset[T]] {
    val tag: TypeTag[T] = implicitly
  }

  final case class DataFrameAction(rows: Seq[Row], schema: StructType) extends StageAction[DataFrame]

  type Stage[A] = FreeApplicative[StageAction, A]

  def dataFrame(rows: Seq[Row], schema: StructType): Stage[DataFrame] =
    lift[StageAction, DataFrame](DataFrameAction(rows, schema))

  def dataset[T: TypeTag](data: Seq[T], strict: Boolean, allowTruncate: Boolean): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](DatasetAction(data, strict, allowTruncate))

  def emptyStage: Stage[Unit] = pure(Unit)

  def read[T: TypeTag](
                        schemaName: String,
                        tableName: String,
                        strict: Boolean
                      ): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](ReadAction(schemaName, tableName, strict))

  def write[T: TypeTag](
                         schemaName: String,
                         tableName: String,
                         strict: Boolean,
                         allowTruncate: Boolean,
                         saveMode: Option[SaveMode]
                       ): Stage[Dataset[T] => Unit] =
    lift[StageAction, Dataset[T] => Unit](WriteAction(schemaName, tableName, strict, allowTruncate, saveMode))

  def stageCompiler: StageAction ~> SparkAction = λ[StageAction ~> SparkAction] {
    case action: DatasetAction[t] => session: SparkSession => {
      val expressionEncoder = ExpressionEncoder[t]()(action.tag)
      val encoder = if (action.strict) {
        enki.adjustUsingMetadata[t](expressionEncoder, action.allowTruncate)(action.tag)
      } else {
        expressionEncoder
      }
      session.createDataset[t](action.data)(encoder)
    }

    case action: ReadAction[t] => session: SparkSession =>
      session.table(s"${action.schemaName}.${action.tableName}").cast[t](action.strict, allowTruncate = false)(action.tag)

    case action: WriteAction[t] => _: SparkSession =>
      (dataset: Dataset[t]) => {
        val writer = dataset.cast[t](action.strict, action.allowTruncate)(action.tag).write

        action.saveMode.foreach(writer.mode)
        writer.saveAsTable(s"${action.schemaName}.${action.tableName}")
      }

    case action: DataFrameAction => session: SparkSession =>
      session.createDataFrame(action.rows, action.schema)
  }

  def stageReads(stage: Stage[_]): Set[ReadAction[_]] = {
    stage.analyze(λ[StageAction ~> λ[α => Set[ReadAction[_]]]] {
      case r: ReadAction[_] => Set(r)
      case _ => Set.empty
    })
  }

  def stageWrites(stage: Stage[_]): Set[WriteAction[_]] = {
    stage.analyze(λ[StageAction ~> λ[α => Set[WriteAction[_]]]] {
      case r: WriteAction[_] => Set(r)
      case _ => Set.empty
    })
  }

  def stageNonEmpty(stage: Stage[_]): Boolean = {
    stage.analyze(λ[StageAction ~> λ[α => Option[Unit]]] {
      case _ => Some(Unit)
    }).nonEmpty
  }
}