package enki

import cats._
import org.apache.spark.sql._
import scalax.collection.Graph
import scalax.collection.GraphEdge._
import scalax.collection.GraphPredef._

trait GraphModule {

  case class ActionGraph(graph: Graph[String, DiEdge], actions: Map[String, SparkAction[Unit]]) {
    private def checkActionExists(name: String): Unit = {
      if (!actions.contains(name)) {
        throw new Exception(s"Action $name not found.")
      }
    }

    private def validate(): Unit = {
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

    def resume(name: String)(implicit session: SparkSession): Unit = {
      checkActionExists(name)
      linearized.dropWhile(_ != name).foreach(runAction)
    }

    def runAction(name: String)(implicit session: SparkSession): Unit = {
      checkActionExists(name)
      try {
        //TODO: stack descriptions
        session.sparkContext.setJobDescription(name)
        actions(name)(session)
      } finally {
        session.sparkContext.setJobDescription(null)
      }
    }

    def linearized: Seq[String] = graph.topologicalSort.fold(
      cycleNode => throw new Exception(s"Circular dependency found at ${cycleNode.value}"),
      order => order.toList.reverse.map(_.value)
    )

    def runAll(implicit session: SparkSession): Unit = {
      validate()
      linearized.foreach(runAction)
    }
  }

  implicit val actionGraphMonoid: Monoid[ActionGraph] = new Monoid[ActionGraph] {
    override def empty: ActionGraph = ActionGraph(Graph.empty[String, DiEdge], Map.empty[String, SparkAction[Unit]])

    override def combine(x: ActionGraph, y: ActionGraph): ActionGraph = ActionGraph(x.graph ++ y.graph, x.actions ++ y.actions)
  }
}