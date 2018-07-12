package enki

trait StageModule {
  this: ActionModule =>

  import cats._
  import cats.instances.all._
  import cats.free.FreeApplicative
  import cats.free.FreeApplicative._
  import org.apache.spark.sql._

  import scala.reflect.runtime.universe.TypeTag

  //TODO: возможно следует вычислять зависимости из ридера.
  //но для этого надо понимать, является ли эта зависимость "внешней" или "внутренней".
  sealed trait StageAction[T]

  final case class ReadAction[T](database: Database, table: String, dependencies: Set[String]) extends StageAction[Dataset[T]]

  final case class WriteAction[T](database: Database, table: String) extends StageAction[Dataset[T] => Unit]

  final case class DataAction[T](data: Seq[T]) extends StageAction[Dataset[T]]

  type Stage[A] = FreeApplicative[StageAction, A]

  def data[T: TypeTag](data: Seq[T]): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](DataAction(data))

  def read[T: TypeTag](database: Database, tableName: String): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](ReadAction(database, tableName, Set.empty))

  def read[T: TypeTag](database: Database, tableName: String, dependency: String): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](ReadAction(database, tableName, Set(dependency)))

  def write[T](database: Database, tableName: String): Stage[Dataset[T] => Unit] =
    lift[StageAction, Dataset[T] => Unit](WriteAction(database, tableName))

  def stageCompiler: StageAction ~> SparkAction = λ[StageAction ~> SparkAction] {
    case DataAction(data) => dataAction(data)
    case ReadAction(database, table, _) => readAction(database, table)
    case WriteAction(database, table) => writeAction(database, table)
  }

  def stageDependencies(stage: Stage[_]): Set[String] = {
    stage.analyze(λ[StageAction ~> λ[α => Set[String]]] {
      case ReadAction(_, _, d) => d
      case _ => Set.empty
    })
  }
}