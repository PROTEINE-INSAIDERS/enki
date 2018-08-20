package enki

import cats._
import cats.implicits._
import org.apache.spark.sql._
import scalax.collection.Graph
import scalax.collection.GraphEdge._

import scala.annotation.tailrec

trait GraphModule {

  case class ActionGraph(graph: Graph[String, DiEdge], actions: Map[String, Either[ActionGraph, Stage[_]]]) {
    private def splitPath(pathStr: String): List[String] = pathStr.split("->").toList

    private def checkActionExists(name: String): Unit = {
      get(name)
    }

    private def get(name: String): Either[ActionGraph, Stage[_]] = getOpt(splitPath(name)) match {
      case Some(a) => a
      case None => throw new Exception(s"Action $name not found.")
    }

    @tailrec private def getOpt(path: List[String]): Option[Either[ActionGraph, Stage[_]]] = path match {
      case Nil => None
      case x :: Nil => actions.get(x)
      case x :: xs => actions.get(x) match {
        case Some(Left(g)) => g.getOpt(xs)
        case _ => None
      }
    }

    def getOpt(pathStr: String): Option[Either[ActionGraph, Stage[_]]] = getOpt(splitPath(pathStr))

    private def subGraphs: Seq[ActionGraph] = {
      actions.values.collect { case Left(ag) => ag }.toSeq
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
          case Left(g) => g.runAll(session, _ => compiler)
          case Right(a) => a.foldMap(compiler).apply(session)
        }
      } finally {
        session.sparkContext.setJobDescription(null)
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
      ActionGraph(Graph[String, DiEdge](stageName), Map(stageName -> Right[ActionGraph, Stage[_]](stage)))
    }

    /**
      * Create action graph with single subgraph.
      */
    def apply(stageName: String, subGraph: ActionGraph): ActionGraph = {
      ActionGraph(Graph[String, DiEdge](stageName), Map(stageName -> Left[ActionGraph, Stage[_]](subGraph)))
    }

    def empty: ActionGraph = ActionGraph(Graph.empty[String, DiEdge], Map.empty[String, Either[ActionGraph, Stage[_]]])
  }

  implicit val actionGraphMonoid: Monoid[ActionGraph] = new Monoid[ActionGraph] {
    override def empty: ActionGraph = ActionGraph.empty

    override def combine(x: ActionGraph, y: ActionGraph): ActionGraph = ActionGraph(x.graph ++ y.graph, x.actions ++ y.actions)
  }
}