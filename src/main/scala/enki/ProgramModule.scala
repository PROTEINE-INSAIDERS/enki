package enki

trait ProgramModule {
  this: GraphModule =>

  import cats._
  import cats.implicits._
  import cats.data._
  import cats.free.Free._
  import cats.free._
  import cats.implicits._
  import org.apache.spark.sql._
  import scalax.collection.Graph
  import scalax.collection.GraphPredef._

  import scala.reflect.runtime.universe.TypeTag

  sealed trait ProgramAction[A]

  final case class PersistAction[T: TypeTag](tableName: String,
                                             stage: Stage[Dataset[T]],
                                             database: Database) extends ProgramAction[Stage[Dataset[T]]] {
    private[ProgramModule] def tag: TypeTag[T] = implicitly[TypeTag[T]]
  }

  type Program[A] = Free[ProgramAction, A]

  def persist[T: TypeTag](tableName: String)(stage: Stage[Dataset[T]])(implicit db: Database): Program[Stage[Dataset[T]]] =
    liftF[ProgramAction, Stage[Dataset[T]]](PersistAction[T](
      tableName,
      stage,
      db))

  type StageWriter[A] = Writer[List[(String, Stage[Unit])], A]

  val programSplitter: ProgramAction ~> StageWriter = λ[ProgramAction ~> StageWriter] {
    case p: PersistAction[t] => {
      val stageName = s"${p.database.schema}.${p.tableName}"
      val stage = p.stage ap write[t](p.tableName)(p.database)
      for {
        _ <- List((stageName, stage)).tell
      } yield {
        read[t](p.tableName, p.database, stageName)(p.tag)
      }
    }
  }

  def buildActionGraph(name: String, p: Program[Stage[Unit]]): ActionGraph = {
    val (stages, lastStage) = p.foldMap(programSplitter).run
    ((name, lastStage) :: stages)
      .foldMap { case (name, stage) =>
        val action = stage.foldMap(stageCompiler)
        val dependencies = stageDependencies(stage)
        ActionGraph(Graph(dependencies.toSeq.map(name ~> _): _*), Map(name -> action))
      }
  }
}
