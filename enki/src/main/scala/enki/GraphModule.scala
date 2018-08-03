package enki

import cats._
import cats.implicits._
import org.apache.spark.sql._
import scalax.collection.Graph
import scalax.collection.GraphEdge._
import scalax.collection.GraphPredef._

trait GraphModule {

  case class ActionGraph(graph: Graph[String, DiEdge], actions: Map[String, Stage[_]]) {
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

    def addAction(name: String, action: Stage[_]): ActionGraph = {
      copy(actions = actions + (name -> action))
    }

    def addAction(name: String, action: Stage[_], dependsOn: String*): ActionGraph = {
      copy(actions = actions + (name -> action), graph = graph ++ dependsOn.map(name ~> _))
    }

    //TODO: по идее зависимости можно доставать прямо из Stage, следует ли использовать для них отдельный метод?
    def addDependency(from: String, to: String): ActionGraph = {
      copy(graph = graph + (from ~> to))
    }

    def resume(name: String, session: SparkSession, compilers: String => StageAction ~> SparkAction): Unit = {
      checkActionExists(name)
      linearized.dropWhile(_ != name).foreach { stageName =>
        runAction(stageName, session, compilers(stageName))
      }
    }

    def runAction(name: String, session: SparkSession, compiler: StageAction ~> SparkAction): Unit = {
      checkActionExists(name)
      try {
        //TODO: stack descriptions
        session.sparkContext.setJobDescription(name)
        val action = actions(name).foldMap(compiler)
        action(session)
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

  implicit val actionGraphMonoid: Monoid[ActionGraph] = new Monoid[ActionGraph] {
    override def empty: ActionGraph = ActionGraph(Graph.empty[String, DiEdge], Map.empty[String, Stage[_]])

    override def combine(x: ActionGraph, y: ActionGraph): ActionGraph = ActionGraph(x.graph ++ y.graph, x.actions ++ y.actions)
  }
}