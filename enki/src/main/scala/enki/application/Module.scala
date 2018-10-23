package enki
package application

import cats._
import cats.implicits._
import com.monovore.decline._
import enki.internal._
import org.apache.spark.sql._

trait Module {
  //TODO: parametrize module explicictly
  //TODO: implement spark variable substitution support.
  self: Enki =>

  implicit val injectOldArg: ArgAlg.Op :<: StageOp
  implicit val sparkInjection: SparkAlg.Op :<: StageOp

  trait EnkiMain {
    protected def actionParams(nodes: ActionNode*): Opts[Map[String, ParameterValue]] = {
      val oldArgs = Monoid.combineAll(nodes.map(_.analyzeIn(ArgToOpts.analyzer))).opts
      val sparkArgs = Monoid.combineAll(nodes.map(_.analyzeIn(enki.spark.ArgToOpts.analyzer))).opts
      // merge spark arguments with enki arguments.
      (oldArgs, sparkArgs) mapN { (a, sa) => a ++ sa.filter { p => !a.contains(p._1) }.mapValues(StringValue) }
    }

    protected def actionGraph: ActionGraph

    protected def session: Opts[SparkSession] = Opts(SparkSession.builder().getOrCreate())

    protected def compiler: Opts[SparkSession => StageHandler] = Opts(_ => stageHandler)

    protected def resume(action: String): Opts[SparkSession => (SparkSession => StageHandler) => Unit] = Opts {
      (params: Map[String, ParameterValue]) =>
        (session: SparkSession) =>
          (compiler: SparkSession => StageHandler) =>
            actionGraph.resume(action, compiler(session), Environment(session, params))
    } <*> actionParams(actionGraph.resumeStages(action): _*) //TODO: нужно собрать параметры из всех последующих нод.

    protected def run(action: String): Opts[SparkSession => (SparkSession => StageHandler) => Unit] = Opts {
      (params: Map[String, ParameterValue]) =>
        (session: SparkSession) =>
          (compiler: SparkSession => StageHandler) =>
            actionGraph.runAction(action, compiler(session), Environment(session, params))
    } <*> actionParams(actionGraph(action))

    protected def runAll: Opts[SparkSession => (SparkSession => StageHandler) => Unit] = Opts {
      (params: Map[String, ParameterValue]) =>
        (session: SparkSession) =>
          (compiler: SparkSession => StageHandler) =>
            actionGraph.runAll(compiler(session), Environment(session, params))
    } <*> actionParams(GraphNode(actionGraph))

    protected def listCommand: Opts[Unit] = Opts.subcommand(name = "list", help = "List all actions.") {
      Opts(actionGraph.linearized.foreach(println))
    }

    protected def resumeCommand: Opts[Unit] = Opts.subcommand(name = "resume", help = "Resume computation from specified stage.") {
      actionGraph.linearized.map { action =>
        Opts.subcommand(
          name = action,
          help = s"Run $action."
        ) {
          resume(action) <*> session <*> compiler
        }
      }.reduce(_.orElse(_))
    }

    protected def runCommand: Opts[Unit] = Opts.subcommand(name = "run", help = "Execute specified action.") {
      actionGraph.linearized.map { action =>
        Opts.subcommand(
          name = action,
          help = s"Run $action."
        ) {
          run(action) <*> session <*> compiler
        }
      }.reduce(_.orElse(_))
    }

    protected def runAllCommand: Opts[Unit] = Opts.subcommand(name = "runAll", help = "Execute all actions.") {
      runAll <*> session <*> compiler
    }

    def main: Opts[Unit] = listCommand orElse resumeCommand orElse runCommand orElse runAllCommand
  }

  object EnkiMain {
    def apply(
               actionGraph: ActionGraph,
               session: SparkSession = SparkSession.builder().getOrCreate()
             ): EnkiMain = {
      val g = actionGraph
      val s = session
      new EnkiMain {
        override protected def actionGraph: ActionGraph = g

        override protected def session: Opts[SparkSession] = Opts(s)
      }
    }
  }

  implicit def enkiMainToOpts(enkiMain: EnkiMain): Opts[Unit] = enkiMain.main
}