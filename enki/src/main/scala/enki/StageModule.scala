package enki

import cats._
import cats.free.FreeApplicative
import cats.free.FreeApplicative._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait StageModule {
  this: ActionModule =>

  //TODO: возможно следует вычислять зависимости из ридера.
  //но для этого надо понимать, является ли эта зависимость "внешней" или "внутренней".
  sealed trait StageAction[T]

  final case class ReadAction[T: TypeTag](reader: (SparkSession, String, String) => DataFrame,
                                          schemaName: String,
                                          tableName: String,
                                          restricted: Boolean,
                                          dependencies: Set[String]
                                         ) extends StageAction[Dataset[T]] {
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

  def read[T: TypeTag](reader: (SparkSession, String, String) => DataFrame,
                       schemaName: String,
                       tableName: String,
                       restricted: Boolean,
                       dependencies: Set[String]): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](ReadAction(reader, schemaName, tableName, restricted, Set.empty))

  def write[T](database: Database, tableName: String): Stage[Dataset[T] => Unit] =
    lift[StageAction, Dataset[T] => Unit](WriteAction(database, tableName))

  def stageCompiler: StageAction ~> SparkAction = λ[StageAction ~> SparkAction] {
    case action: DatasetAction[t] => datasetAction[t](action.data)(action.tag)

    case action: ReadAction[t] => session: SparkSession => {
      if (typeOf[t] == typeOf[Row]) {
        if (action.restricted) {
          throw new Exception("Unable to restrict schema for generic type Row.")
        }
        action.reader(session, action.schemaName, action.tableName).asInstanceOf[Dataset[t]]
      }
      else {
        val encoder = ExpressionEncoder[t]
        val table = action.reader(session, action.schemaName, action.tableName)
        if (action.restricted) {
          table.select(encoder.schema.map(f => table(f.name)): _*).as[t](encoder)
        } else {
          table.as[t](encoder)
        }
      }
    }

    case action: WriteAction[t] => writeAction(action.database, action.table)

    case action: DataFrameAction => dataFrameAction(action.rows, action.schema)
  }

  def stageDependencies(stage: Stage[_]): Set[String] = {
    stage.analyze(λ[StageAction ~> λ[α => Set[String]]] {
      case ReadAction(_, _, _, d) => d
      case _ => Set.empty
    })
  }
}