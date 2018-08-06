package enki

import scalax.collection.Graph
import scalax.collection.GraphEdge._
import scalax.collection.GraphPredef._

object OozieGenerator {

  val sampleGraph = Graph("C" ~> "D", "A" ~> "B", "A" ~> "C", DiEdge("G", "D"), "FFF")

  def genFork(name: String, nodes: Seq[String]): String = {
    val nnn = nodes.map(node => s"""    <path start="$node"/>""").mkString("\n")
    s"""
       |<fork name="$name">
       |$nnn
       |</fork>
       | """.stripMargin
  }


  def action(name: String, to: String): String =
    s"""
       |<action name="$name">
       |    <shell xmlns="uri:oozie:shell-action:0.2">
       |        <job-tracker>$${jobTracker}</job-tracker>
       |        <name-node>$${nameNode}</name-node>
       |        <exec>./run-stage.sh</exec>
       |        <env-var>userName=$${wf: user()}</env-var>
       |        <env-var>jarName=$${jarName}</env-var>
       |        <env-var>domain=$${domain}</env-var>
       |        <env-var>numExecutors=$${numExecutors}</env-var>
       |        <env-var>businessDate=$${businessDate}</env-var>
       |        <env-var>ctl=$${ctl}</env-var>
       |        <env-var>yarnQueue=$${yarnQueue}</env-var>
       |        <env-var>loading_id=$${loading_id}</env-var>
       |        <env-var>sparkHome=$${sparkHome}</env-var>
       |        <env-var>rootPath=$${rootPath}</env-var>
       |        <env-var>marketSchema=$${marketSchema}</env-var>
       |        <env-var>publish=true</env-var>
       |        <env-var>classPath=ru.sberbank.bigdata.overdrafts.limits.S10LimitOverdraftView</env-var>
       |        <env-var>wfName=$name</env-var>
       |        <file>$${keytabPath}#keytab</file>
       |        <file>run-stage.sh#run-stage.sh</file>
       |        <file>./lib/$${jarName}#$${jarName}</file>
       |    </shell>
       |    <ok to="$to"/>
       |    <error to="kill"/>
       |</action>
       |""".stripMargin

  def genJoinNode(name: String, to: String): String =
    s"""
       |<join name="$name" to="$to"/>
       |""".stripMargin

  def genOozieWf(graph: Graph[String, DiEdge]): String = {

    val reversed = Graph(graph.edges.map(edge => edge.to.value ~> edge.from.value).toSeq ++ graph.nodes: _*)

    val layers = reversed.topologicalSort.fold(
      cycleNode => throw new Exception(s"Circular dependency found at ${cycleNode.value}"),
      _.toLayered
    )
      .map { case (a, b) => (a, b.map(_.value).toList) }
      .toList

    val actions: List[((Int, List[String]), (Int, List[String]))] = layers.zip(layers.tail :+ (layers.size, List("end")))

//    def genFork(level: Int, strings: List[String]): String = s"""fork_$level , to "${strings.mkString(", ")}"\n"""

//    def genJoinNode(level: Int, joinTo: String) = s"join_$level , to $joinTo"

    val start: String = if (layers.head._2.size == 1) {
      s"""<start to="${layers.head._2.head}">"""
    } else {
      s"""<start to="fork_${layers.head._1}>"\n""" +
      genFork(s"fork_${layers.head._1}", layers.head._2)
    }

    start +
    actions.map { case ((currLevel, currNodes), (nextLevel, nextNodes)) =>

      val (nextLevelNode: String, forkNode: String) =
        if (nextNodes.size == 1) (nextNodes.head, "")
        else (s"fork_$nextLevel", genFork(s"fork_$nextLevel", nextNodes))

      val (currTo, currJoin) = if (currNodes.size == 1) (nextLevelNode, "")
      else (s"join_$currLevel", genJoinNode(s"join_$currLevel", nextLevelNode))

      val actions = currNodes.map(node =>
        action(node, currTo)
      ).mkString("\n")

      actions + currJoin + forkNode

    }.mkString("\n")

  }

  def main(args: Array[String]): Unit = {
    val oozieWf = genOozieWf(sampleGraph)

    println(oozieWf)
  }
}
