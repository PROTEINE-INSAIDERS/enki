package enki
package application

import cats._
import cats.implicits._
import com.monovore.decline._
import enki.internal._
import org.apache.spark.sql._

trait Module {
  //TODO: parametrize module explicictly
  self: Enki =>

  implicit val injectArg: ArgAlg.Op :<: StageOp

  trait EnkiMain {
    protected def actionParams(node: ActionNode): Opts[Map[String, ParameterValue]] =
      node.analyzeIn(ArgToOpts.analyzer).opts

    protected def actionGraph: ActionGraph

    protected def session: Opts[SparkSession]

    protected def compiler: Opts[StageHandler] = Opts(stageHandler)

    protected def resume(action: String): Opts[SparkSession => StageHandler => Unit] = Opts {
      (params: Map[String, ParameterValue]) =>
        (session: SparkSession) =>
          (compiler: StageHandler) =>
            actionGraph.resume(action, compiler, Environment(session, params))
    } <*> actionParams(actionGraph(action))

    protected def run(action: String): Opts[SparkSession => StageHandler => Unit] = Opts {
      (params: Map[String, ParameterValue]) =>
        (session: SparkSession) =>
          (compiler: StageHandler) =>
            actionGraph.runAction(action, compiler, Environment(session, params))
    } <*> actionParams(actionGraph(action))

    protected def runAll: Opts[SparkSession => StageHandler => Unit] = Opts {
      (params: Map[String, ParameterValue]) =>
        (session: SparkSession) =>
          (compiler: StageHandler) =>
            actionGraph.runAll(compiler, Environment(session, params))
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