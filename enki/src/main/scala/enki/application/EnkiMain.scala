package enki.application

import cats.data._
import cats.implicits._
import com.monovore.decline._
import enki._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import freestyle.free._
import freestyle.free.implicits._
import scala.collection.mutable
import scala.util.Try

trait EnkiMainWrapper {
  self: Enki =>

  //TODO: factor out Environment as only parametrized entity (should compiles be parametrized?)
  trait EnkiMain {
    protected def actionParams(node: ActionNode): Opts[Map[String, ParameterValue]] = {

      val arguments = node.analyze(stageArguments(_, Set(_)))

      val argumentMap = mutable.Map[String, ArgumentAction]()

      arguments.foreach { arg =>
        argumentMap.get(arg.name) match {
          case None => argumentMap += (arg.name -> arg)
          case Some(other) =>
            if (other.dataType != arg.dataType)
              throw new Exception(s"Argument ${arg.name} is already added with different type.")
            if (other.defaultStringValue != arg.defaultStringValue)
              throw new Exception(s"Argument ${arg.name} is already added with different default value.")
        }
      }

      //TODO: replace with argsToOpts

      argumentMap.values.toList.traverse { (arg: ArgumentAction) =>
        val opt = Opts.option[String](long = arg.name, help = arg.description)
        (arg.defaultStringValue match {
          case Some(value) => opt.withDefault(value)
          case None => opt
        }).mapValidated { (value: String) =>
          arg.dataType match {
            case StringType => Validated.valid((arg.name, StringValue(value).asInstanceOf[ParameterValue]))
            case IntegerType => Try(value.toInt).toOption match {
              case Some(intValue) => Validated.valid((arg.name, IntegerValue(intValue).asInstanceOf[ParameterValue]))
              case None => Validated.invalidNel(s"Unable to convert parameter's ${arg.name} value $value to integer.")
            }
          }
        }
      }.map(_.toMap)
    }

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