package enki

import cats.data._
import com.monovore.decline._
import org.apache.spark.sql._

trait ApplicationModule {

  def defaultMain(
                   actionGraph: ActionGraph,
                   session: SparkSession = SparkSession.builder().getOrCreate()
                 ): Opts[Unit] = new EnkiMain {
    override def actionGraph: enki.ActionGraph = actionGraph

    override def session: SparkSession = session
  }.main

  trait EnkiMain {
    protected def actionGraph: ActionGraph

    protected def session: SparkSession

    protected def list: Opts[Unit] = Opts.subcommand(
      name = "list",
      help = "List all stages."
    )(Opts {
      actionGraph.linearized.foreach(println)
    })

    protected def run: Opts[Unit] = Opts.subcommand(
      name = "run",
      help = "Execute specified stage."
    )(Opts.arguments[String]("stage").mapValidated { stages =>
      stages.filterNot(actionGraph.actions.contains) match {
        case Nil => Validated.valid(stages)
        case missing => Validated.invalidNel(s"Stages(s) ${missing.mkString(", ")} not found!")
      }
    } map { stage =>
      stage.toList.foreach { stageName => actionGraph.runAction(stageName, session, stageCompiler) }
    })

    protected def resume: Opts[Unit] = Opts.subcommand(
      name = "resume",
      help = "Resume computation from specified stage."
    )(Opts.argument[String]("action").mapValidated { stage =>
      if (actionGraph.actions.contains(stage)) {
        Validated.valid(stage)
      } else {
        Validated.invalidNel(s"Stage $stage not found!")
      }
    } map { stage =>
      actionGraph.resume(stage, session, _ => stageCompiler)
    })

    protected def runAll: Opts[Unit] = Opts.subcommand(
      name = "runAll",
      help = "Execute all stages."
    )(Opts {
      actionGraph.runAll(session)
    })

    def main: Opts[Unit] = list orElse resume orElse run orElse runAll
  }
}
