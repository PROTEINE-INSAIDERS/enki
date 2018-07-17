package enki

import java.sql.Struct

import cats._
import cats.implicits._
import cats.free.FreeApplicative
import cats.free.FreeApplicative._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe.TypeTag

trait StageModule {
  this: ActionModule =>

  //TODO: возможно следует вычислять зависимости из ридера.
  //но для этого надо понимать, является ли эта зависимость "внешней" или "внутренней".
  sealed trait StageAction[T]

  final case class ReadAction[T: TypeTag](database: Database, table: String, dependencies: Set[String]) extends StageAction[Dataset[T]] {
    val tag: TypeTag[T] = implicitly
  }

  final case class WriteAction[T](database: Database, table: String) extends StageAction[Dataset[T] => Unit]

  final case class DatasetAction[T: TypeTag](data: Seq[T]) extends StageAction[Dataset[T]] {
    val tag: TypeTag[T] = implicitly
  }

  final case class DataFrameAction(rows: Seq[Row], schema: StructType) extends StageAction[DataFrame]

  type Stage[A] = FreeApplicative[StageAction, A]

  def dataFrame(rows: Seq[Row], schema: StructType): Stage[DataFrame] =
    lift[StageAction, DataFrame](DataFrameAction(rows, schema))

  def dataset[T: TypeTag](data: Seq[T]): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](DatasetAction(data))

  def read[T: TypeTag](database: Database, tableName: String): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](ReadAction(database, tableName, Set.empty))

  def read[T: TypeTag](database: Database, tableName: String, dependency: String): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](ReadAction(database, tableName, Set(dependency)))

  def write[T](database: Database, tableName: String): Stage[Dataset[T] => Unit] =
    lift[StageAction, Dataset[T] => Unit](WriteAction(database, tableName))

  def stageCompiler: StageAction ~> SparkAction = λ[StageAction ~> SparkAction] {
    case action: DatasetAction[t] => datasetAction[t](action.data)(action.tag)
    case action: ReadAction[t] => readAction[t](action.database, action.table)(action.tag)
    case action: WriteAction[t] => writeAction(action.database, action.table)
    case action: DataFrameAction => dataFrameAction(action.rows, action.schema)
  }

  def stageDependencies(stage: Stage[_]): Set[String] = {
    stage.analyze(λ[StageAction ~> λ[α => Set[String]]] {
      case ReadAction(_, _, d) => d
      case _ => Set.empty
    })
  }
}