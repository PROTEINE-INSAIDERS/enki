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

  //TODO: вынести action в базовый класс??
  final case class ReadAction[T](action: SparkAction[Dataset[T]], dependencies: Set[String]) extends StageAction[Dataset[T]]

  final case class WriteAction[T](action: SparkAction[Dataset[T] => Unit]) extends StageAction[Dataset[T] => Unit]

  //TODO: добавить действие по созданию датасета из Seq.
  //TODO: можно даже общее дейстиве по созданию датасета из Session.

  type Stage[A] = FreeApplicative[StageAction, A]

  def read[T: TypeTag](database: Database, tableName: String): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](ReadAction(readAction(database, tableName), Set.empty))

  def read[T: TypeTag](database: Database, tableName: String, dependency: String): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](ReadAction(readAction(database, tableName), Set(dependency)))

  def write[T](database: Database, tableName: String): Stage[Dataset[T] => Unit] =
    lift[StageAction, Dataset[T] => Unit](WriteAction(writeAction(database, tableName)))

  def stageCompiler: StageAction ~> SparkAction = λ[StageAction ~> SparkAction] {
    case ReadAction(a, _) => a
    case WriteAction(a) => a
  }

  def stageDependencies(stage: Stage[_]): Set[String] = {
    stage.analyze(λ[StageAction ~> λ[α => Set[String]]] {
      case ReadAction(_, d) => d
      case WriteAction(_) => Set.empty
    })
  }
}