package enki

import alleycats.std.iterable._
import cats._
import cats.implicits._
import org.apache.spark.sql._
import scalax.collection.Graph
import scalax.collection.GraphEdge._

import scala.annotation.tailrec

//TODO: Try cata to build dependency graph in form of annotations.

trait GraphModule {

  sealed trait ActionNode {
    def analyze[M: Monoid](f: Stage[_] => M): M

    def reads[M: Monoid](f: ReadTableAction => M): M = analyze(stageReads(_, f))

    def writes[M: Monoid](f: WriteTableAction => M): M = analyze(stageWrites(_, f))

    def externalReads[M: Monoid](f: ReadTableAction => M): M = {
      val writeTables = writes(w => Set((w.schemaName, w.tableName)))
      reads(r => if (writeTables.contains((r.schemaName, r.tableName))) {
        implicitly[Monoid[M]].empty
      } else {
        f(r)
      })
    }
  }

  final case class StageNode(stage: Stage[_]) extends ActionNode {
    override def analyze[M: Monoid](f: Stage[_] => M): M = f(stage)
  }

  final case class GraphNode(graph: ActionGraph) extends ActionNode {
    override def analyze[M: Monoid](f: Stage[_] => M): M = graph.analyze(f)
  }

  //TODO: возможно граф зависимостей нужно строить не в процессе сборки графа, а выводить из actions по запросу.
  case class ActionGraph(graph: Graph[String, DiEdge], actions: Map[String, ActionNode]) {
    private def splitPath(pathStr: String): List[String] = pathStr.split("->").toList

    private def checkActionExists(name: String): Unit = {
      get(name)
    }

    private def get(name: String): ActionNode = getOpt(splitPath(name)) match {
      case Some(a) => a
      case None => throw new Exception(s"Action $name not found.")
    }

    @tailrec private def getOpt(path: List[String]): Option[ActionNode] = path match {
      case Nil => None
      case x :: Nil => actions.get(x)
      case x :: xs => actions.get(x) match {
        case Some(GraphNode(g)) => g.getOpt(xs)
        case _ => None
      }
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

    def resume(name: String, session: SparkSession, compilers: String => StageAction ~> SparkAction): Unit = {
      checkActionExists(name)
      linearized.dropWhile(_ != name).foreach { stageName =>
        runAction(stageName, session, compilers(stageName))
      }
    }

    def runAction(name: String, session: SparkSession, compiler: StageAction ~> SparkAction): Unit = {
      try {
        //TODO: stack descriptions
        session.sparkContext.setJobDescription(name)
        get(name) match {
          case GraphNode(g) => g.runAll(session, _ => compiler)
          case StageNode(a) => a.foldMap(compiler).apply(session)
        }
      } finally {
        session.sparkContext.setJobDescription(null)
      }
    }

    def analyze[M: Monoid](f: Stage[_] => M): M = {
      actions.values.foldMap {
        case StageNode(s) => f(s)
        case GraphNode(g) => g.analyze(f)
      }
    }

    def linearized: Seq[String] = graph.topologicalSort.fold(
      cycleNode => throw new Exception(s"Circular dependency found at ${cycleNode.value}"),
      order => order.toList.reverse.map(_.value)
    )

    def runAll(session: SparkSession, compilers: String => StageAction ~> SparkAction): Unit = {
      validate()
      linearized.foreach { stageName => runAction(stageName, session, compilers(stageName)) }
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