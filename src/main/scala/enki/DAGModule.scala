package enki

trait DAGModule {

  import cats._
  import cats.data._
  import cats.free.Free._
  import cats.free._
  import cats.implicits._
  import org.apache.spark.sql._
  import org.apache.spark.sql.catalyst.encoders._
  import scalax.collection.Graph
  import scalax.collection.GraphEdge._
  import scalax.collection.GraphPredef._

  import scala.reflect.runtime.universe.{TypeTag, typeOf}

  type SparkAction[A] = SparkSession => A

  case class ActionGraph(graph: Graph[String, DiEdge], actions: Map[String, SparkAction[Unit]]) {
    private def checkActionExists(name: String): Unit = {
      if (!actions.contains(name)) {
        throw new Exception(s"Action $name not found.")
      }
    }

    private def validate() = {
      graph.edges.foreach { e =>
        checkActionExists(e.from.value)
        checkActionExists(e.to.value)
      }
      graph.findCycle.map { cycle =>
        throw new Exception(s"Circular dependency: $cycle")
      }
    }

    def addAction(name: String, action: SparkAction[Unit]): ActionGraph = {
      copy(actions = actions + (name -> action))
    }

    def addAction(name: String, action: SparkAction[Unit], dependsOn: String*): ActionGraph = {
      copy(actions = actions + (name -> action), graph = graph ++ dependsOn.map(name ~> _))
    }

    def addDependency(from: String, to: String): ActionGraph = {
      copy(graph = graph + (from ~> to))
    }

    def runAction(name: String)(implicit session: SparkSession): Unit = {
      checkActionExists(name)
      actions(name)(session)
    }

    def runAll(implicit session: SparkSession): Unit = {
      validate()
      graph.topologicalSort.right.get.toList.reverseIterator.foreach { v => actions(v.value)(session) }
    }
  }

  implicit val actionGraphMonoid = new Monoid[ActionGraph] {
    override def empty: ActionGraph = ActionGraph(Graph.empty[String, DiEdge], Map.empty[String, SparkAction[Unit]])

    override def combine(x: ActionGraph, y: ActionGraph): ActionGraph = ActionGraph(x.graph ++ y.graph, x.actions ++ y.actions)
  }

  case class Stage[T](dependencies: Set[String], action: SparkAction[T])

  def stage[T](action: SparkAction[T]) = Stage(Set.empty[String], action)

  def read[T: TypeTag](tableName: String)(implicit db: Database): Stage[Dataset[T]] = stage { session =>
    if (typeOf[T] == typeOf[Row])
      db.readTable(session, tableName).asInstanceOf[Dataset[T]]
    else
      db.readTable(session, tableName).as[T](ExpressionEncoder())
  }

  def read[T: TypeTag](tableName: String, db: Database, dependency: String): Stage[Dataset[T]] =
    Stage(Set(dependency), session => {
      if (typeOf[T] == typeOf[Row])
        db.readTable(session, tableName).asInstanceOf[Dataset[T]]
      else
        db.readTable(session, tableName).as[T](ExpressionEncoder())
    })

  def write[T](tableName: String)(implicit db: Database): Stage[Dataset[T] => Unit] = stage { _ =>
    data =>
      db.writeTable(tableName, data.toDF())
  }

  implicit val stageApplicative: Applicative[Stage] = new Applicative[Stage] {
    override def pure[A](x: A): Stage[A] = Stage(Set.empty[String], _ => x)

    // если мы будем освобождать stage, суммировать завимимости можно будет в интерпретирующем функторе.
    override def ap[A, B](ff: Stage[A => B])(fa: Stage[A]): Stage[B] = Stage(
      ff.dependencies ++ fa.dependencies,
      session => ff.action(session)(fa.action(session)))
  }

  sealed trait ProgramAction[A]

  final case class PersistAction[T: TypeTag](tableName: String,
                                             stage: Stage[Dataset[T]],
                                             database: Database) extends ProgramAction[Stage[Dataset[T]]] {
    private[DAGModule] def tag: TypeTag[T] = implicitly[TypeTag[T]]
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
      val stage = stageApplicative.ap(write[t](p.tableName)(p.database))(p.stage)
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
        ActionGraph(Graph(stage.dependencies.toSeq.map(name ~> _): _*), Map(name -> stage.action))
      }
  }
}