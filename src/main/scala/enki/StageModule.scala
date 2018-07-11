package enki

trait StageModule {
  this: ActionModule =>

  import cats._
  import cats.implicits._
  import cats.free.FreeApplicative
  import cats.free.FreeApplicative._
  import org.apache.spark.sql._

  import scala.reflect.runtime.universe.TypeTag

  //TODO: возможно следует вычислять зависимости из ридера.
  //но для этого надо понимать, является ли эта зависимость "внешней" или "внутренней".
  //TODO: convert to concrete actions.
  final case class StageAction[T](action: SparkAction[T], dependencies: Set[String])

  type Stage[A] = FreeApplicative[StageAction, A]

  def stage[T](action: SparkAction[T]): Stage[T] = lift(StageAction(action, Set.empty[String]))

  def read[T: TypeTag](tableName: String)(implicit db: Database): Stage[Dataset[T]] = stage(readAction(db, tableName))

  def read[T: TypeTag](tableName: String, db: Database, dependency: String): Stage[Dataset[T]] =
    lift(StageAction(readAction(db, tableName), Set(dependency)))

  def write[T](tableName: String)(implicit db: Database): Stage[Dataset[T] => Unit] = stage { _ =>
    data =>
      db.writeTable(tableName, data.toDF())
  }

  def stageCompiler: StageAction ~> SparkAction = λ[StageAction ~> SparkAction] {
    case StageAction(a, _) => a
  }

  def stageDependencies(stage: Stage[_]): Set[String] = {
    stage.analyze(λ[StageAction ~> λ[α => Set[String]]] {
      case StageAction(_, d) => d
    })
  }
}