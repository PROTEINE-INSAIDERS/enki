package enki

import cats._
import cats.implicits._
import enki.internal._
import org.apache.spark.sql._
import scalax.collection.Graph
import scalax.collection.GraphEdge._

import scala.annotation.tailrec
import scala.util.control.NonFatal

//TODO: Try cata to build dependency graph in form of annotations.

final case class ActionFailedException(action: String, cause: Throwable) extends Exception(s"Action $action failed.", cause)

trait GraphModule {
  self: Enki =>

  implicit val stageApplicative: Applicative[StageMonad]

  //TODO: перенести в более подходящий модуль
  def createEmptySources(graph: ActionGraph, session: SparkSession): Unit = {
    sources(graph).foreach {
      case action: ReadDatasetAction[t] =>
        session.sql(s"create database if not exists ${action.schemaName}")
        session.emptyDataset[t](action.encoder).write.mode(SaveMode.Ignore).saveAsTable(action.toString)

      case _ => throw new UnsupportedOperationException("Can not create empty table from DataFrame.")
    }
  }

  //TODO: разные ReadTableAction для одной и той же таблицы могут различаться, т.к. могут использовать разные
  // экземпляры энкодеров.
  def sources: ActionGraph => Set[ReadTableAction] = graph => {
    val readers = graph.analyze(action => stageReads(action, action => Set((action.toString, action))))
    val writers = graph.analyze(action => stageWrites(action, action => Set(action.toString)))
    readers.filter { case (name, _) => !writers.contains(name) }.map { case (_, action) => action }
  }

  sealed trait ActionNode {
    def analyze[M: Monoid](f: Stage[_] => M): M

    def analyzeIn[G[_], M: Monoid](f: G ~> λ[α => M])(implicit in: InjectK[G, StageOp]): M

    def reads[M: Monoid](f: ReadTableAction => M): M = analyze(stageReads(_, f))

    def writes[M: Monoid](f: WriteTableAction => M): M = analyze(stageWrites(_, f))

    def mapStages(f: Stage ~> Stage): ActionNode

    def externalReads[M: Monoid](f: ReadTableAction => M): M = {
      val writeTables = writes(w => Set((w.schemaName, w.tableName)))
      reads(r => if (writeTables.contains((r.schemaName, r.tableName))) {
        Monoid.empty[M]
      } else {
        f(r)
      })
    }
  }

  final case class StageNode(stage: Stage[_]) extends ActionNode {
    def analyzeIn[G[_], M: Monoid](f: G ~> λ[α => M])(implicit in: InjectK[G, StageOp]): M = {
      stage.analyzeIn(f)
    }

    override def analyze[M: Monoid](f: Stage[_] => M): M = f(stage)

    override def mapStages(f: Stage ~> Stage): ActionNode = StageNode(f(stage))
  }

  final case class GraphNode(graph: ActionGraph) extends ActionNode {
    def analyzeIn[G[_], M: Monoid](f: G ~> λ[α => M])(implicit in: InjectK[G, StageOp]): M = {
      //TODO: alleycats??
      graph.actions.values.toList.foldMap(_.analyzeIn(f))
    }

    override def analyze[M: Monoid](f: Stage[_] => M): M = graph.analyze(f)

    override def mapStages(f: Stage ~> Stage): ActionNode = GraphNode(graph.copy(actions = graph.actions.mapValues(node => node.mapStages(f))))
  }

  //TODO: возможно граф зависимостей нужно строить не в процессе сборки графа, а выводить из actions по запросу.
  case class ActionGraph(graph: Graph[String, DiEdge], actions: Map[String, ActionNode]) {
    private def splitPath(pathStr: String): List[String] = pathStr.split("->").toList

    private def checkActionExists(name: String): Unit = {
      this (name)
      ()
    }

    @tailrec private def getOpt(path: List[String]): Option[ActionNode] = path match {
      case Nil => None
      case x :: Nil => actions.get(x)
      case x :: xs => actions.get(x) match {
        case Some(GraphNode(g)) => g.getOpt(xs)
        case _ => None
      }
    }

    def apply(name: String): ActionNode = getOpt(splitPath(name)) match {
      case Some(a) => a
      case None => throw new Exception(s"Action $name not found.")
    }

    def getOpt(pathStr: String): Option[ActionNode] = getOpt(splitPath(pathStr))

    private def subGraphs: Seq[ActionGraph] = {
      actions.values.collect { case GraphNode(ag) => ag }.toSeq
    }

    private def validate(): Unit = {
      graph.edges.foreach { e =>
        checkActionExists(e.from.value)
        checkActionExists(e.to.value)
      }
      graph.findCycle.map { cycle =>
        throw new Exception(s"Circular dependency: $cycle")
      }
      subGraphs.foreach(_.validate())
    }

    def resume(action: String, compiler: StageHandler, environment: Environment): Unit = {
      checkActionExists(action)
      linearized.dropWhile(_ != action).foreach { stageName =>
        runAction(stageName, compiler, environment)
      }
    }

    def runAction(name: String, compiler: StageHandler, environment: Environment): Unit = {
      try {
        //TODO: stack descriptions
        environment.session.sparkContext.setJobDescription(name)
        this (name) match {
          case GraphNode(g) => g.runAll(compiler, environment)
          case StageNode(a) =>
            val action = a.foldMap(compiler)
            run(action, environment)
        }
      } catch {
        case NonFatal(e) => throw ActionFailedException(name, e)
      } finally {
        environment.session.sparkContext.setJobDescription(null)
      }
    }

    def analyze[M: Monoid](f: Stage[_] => M): M = {

      actions.values.toList.foldMap {
        case StageNode(s) => f(s)
        case GraphNode(g) => g.analyze(f)
      }
    }

    def linearized: Seq[String] = graph.topologicalSort.fold(
      cycleNode => throw new Exception(s"Circular dependency found at ${cycleNode.value}"),
      order => order.toList.reverse.map(_.value)
    )

    def runAll(compiler: StageHandler, environment: Environment): Unit = {
      validate()
      linearized.foreach { stageName => runAction(stageName, compiler, environment) }
    }
  }

  object ActionGraph {
    /**
      * Create action graph with single stage.
      */
    def apply(stageName: String, stage: Stage[_]): ActionGraph = {
      ActionGraph(Graph[String, DiEdge](stageName), Map(stageName -> StageNode(stage)))
    }

    /**
      * Create action graph with single subgraph.
      */
    def apply(stageName: String, subGraph: ActionGraph): ActionGraph = {
      ActionGraph(Graph[String, DiEdge](stageName), Map(stageName -> GraphNode(subGraph)))
    }

    def empty: ActionGraph = ActionGraph(Graph.empty[String, DiEdge], Map.empty[String, ActionNode])
  }

  implicit val actionGraphMonoid: Monoid[ActionGraph] = new Monoid[ActionGraph] {
    override def empty: ActionGraph = ActionGraph.empty

    // при комбинации можно брать неразрешенные зависимости искать их в другом графе и добавлять в граф зависимостей.
    override def combine(x: ActionGraph, y: ActionGraph): ActionGraph = ActionGraph(x.graph ++ y.graph, x.actions ++ y.actions)
  }
}