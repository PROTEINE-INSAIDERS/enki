package enki

import cats._
import cats.free.FreeApplicative
import cats.free.FreeApplicative._
import cats.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait StageModule {

  sealed trait StageAction[T]

  //TODO: ReadAction и WriteAction могут содержать много настроечных параметров. Необходимо вынести их в ReaderSettings и
  // WriterSettings, чтобы в дальнейшем уменьнить количество рефакторингов при добавлении новых параметров.
  final case class ReadAction[T: TypeTag](
                                           schemaName: String,
                                           tableName: String,
                                           strict: Boolean
                                         ) extends StageAction[Dataset[T]] {
    val tag: TypeTag[T] = implicitly
  }

  final case class WriteAction[T](
                                   schemaName: String,
                                   tableName: String,
                                   saveMode: Option[SaveMode]
                                 ) extends StageAction[Dataset[T] => Unit]

  final case class DatasetAction[T: TypeTag](data: Seq[T]) extends StageAction[Dataset[T]] {
    val tag: TypeTag[T] = implicitly
  }

  final case class DataFrameAction(rows: Seq[Row], schema: StructType) extends StageAction[DataFrame]

  type Stage[A] = FreeApplicative[StageAction, A]

  def dataFrame(rows: Seq[Row], schema: StructType): Stage[DataFrame] =
    lift[StageAction, DataFrame](DataFrameAction(rows, schema))

  def dataset[T: TypeTag](data: Seq[T]): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](DatasetAction(data))

  def read[T: TypeTag](
                        schemaName: String,
                        tableName: String,
                        restricted: Boolean
                      ): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](ReadAction(schemaName, tableName, restricted))

  def write[T](
                schemaName: String,
                tableName: String,
                saveMode: Option[SaveMode]
              ): Stage[Dataset[T] => Unit] =
    lift[StageAction, Dataset[T] => Unit](WriteAction(schemaName, tableName, saveMode))

  def stageCompiler: StageAction ~> SparkAction = λ[StageAction ~> SparkAction] {
    case action: DatasetAction[t] => session: SparkSession =>
      session.createDataset(action.data)(ExpressionEncoder()(action.tag))

    case action: ReadAction[t] => session: SparkSession => {
      if (action.tag.tpe == typeOf[Row]) {
        if (action.strict) {
          throw new Exception("Unable to restrict schema for generic type Row.")
        }
        session.table(s"${action.schemaName}.${action.tableName}").asInstanceOf[Dataset[t]]
      }
      else {
        val encoder = ExpressionEncoder[t]()(action.tag)
        val table = session.table(s"${action.schemaName}.${action.tableName}")
        if (action.strict) {
          //TODO: здесь можно добавить некоторые преобразования на основании метаданных.
          table.select(encoder.schema.map(f => table(f.name)): _*).as[t](encoder)
        } else {
          table.as[t](encoder)
        }
      }
    }

    case action: WriteAction[t] => _: SparkSession =>
      (dataset: Dataset[t]) => {
        val writer = dataset.write

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