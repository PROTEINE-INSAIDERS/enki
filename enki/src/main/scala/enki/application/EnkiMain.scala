package enki.application

import cats._
import cats.data._
import cats.implicits._
import com.monovore.decline._
import enki._
import org.apache.spark.sql._

import scala.util.Try

//TODO: factor out Environment as only parametrized entity (should compiles be parametrized?)
trait EnkiMain {
  private def actionParams(node: ActionNode): Opts[Map[String, ParameterValue]] = {
    node.analyze(stageArguments(_, Set(_))).toList.traverse { (arg: ArgumentAction) =>
      Opts.option[String](long = arg.name, help = "").mapValidated { (value: String) =>
        arg.argumentType match {
          case StringArgument => Validated.valid((arg.name, StringValue(value).asInstanceOf[ParameterValue]))
          case IntegerArgument => Try(value.toInt).toOption match {
            case Some(intValue) => Validated.valid((arg.name, IntegerValue(intValue).asInstanceOf[ParameterValue]))
            case None => Validated.invalidNel(s"Unable to convert parameter's ${arg.name} value $value to integer.")
          }
        }
      }
    }.map(_.toMap)
  }

  protected def actionGraph: ActionGraph

  protected def session: Opts[SparkSession]

  protected def compiler: Opts[StageAction ~> SparkAction]

  protected def resume1(action: String): Opts[SparkSession => StageAction ~> SparkAction => Unit] = Opts {
    (params: Map[String, ParameterValue]) =>
      (session: SparkSession) =>
        (compiler: StageAction ~> SparkAction) =>
          actionGraph.resume(action, compiler, Environment(session, params))
  } <*> actionParams(actionGraph(action))

  protected def run1(action: String): Opts[SparkSession => StageAction ~> SparkAction => Unit] = Opts {
    (params: Map[String, ParameterValue]) =>
      (session: SparkSession) =>
        (compiler: StageAction ~> SparkAction) =>
          actionGraph.runAction(action, compiler, Environment(session, params))
  } <*> actionParams(actionGraph(action))

  protected def runAll1: Opts[SparkSession => StageAction ~> SparkAction => Unit] = Opts {
    (params: Map[String, ParameterValue]) =>
      (session: SparkSession) =>
        (compiler: StageAction ~> SparkAction) =>
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
        resume1(action) <*> session <*> compiler
      }
    }.reduce(_.orElse(_))
  }

  protected def runCommand: Opts[Unit] = Opts.subcommand(name = "run", help = "Execute specified action.") {
    actionGraph.linearized.map { action =>
      Opts.subcommand(
        name = action,
        help = s"Run $action."
      ) {
        run1(action) <*> session <*> compiler
      }
    }.reduce(_.orElse(_))
  }

  protected def runAllCommand: Opts[Unit] = Opts.subcommand(name = "runAll", help = "Execute all actions.") {
    runAll1 <*> session <*> compiler
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
      override protected def actionGraph = g

      override protected def session: Opts[SparkSession] = Opts(s)

      override protected def compiler: Opts[enki.StageAction ~> SparkAction] = Opts(stageCompiler)
    }
  }
}