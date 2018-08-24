package enki

import cats._
import cats.data._
import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._

trait ApplicationModule {

  def defaultMain(
                   actionGraph: ActionGraph,
                   session: SparkSession = SparkSession.builder().getOrCreate()
                 ): Opts[Unit] = {
    val g = actionGraph
    val s = session
    new EnkiMain {
      override protected def actionGraph: Opts[enki.ActionGraph] = Opts(g)

      override protected def session: Opts[SparkSession] = Opts(s)

      override protected def compiler: Opts[enki.StageAction ~> SparkAction] = Opts(stageCompiler)
    }.main
  }

  trait EnkiMain {
    protected def actionGraph: Opts[ActionGraph]

    protected def session: Opts[SparkSession]

    protected def compiler: Opts[StageAction ~> SparkAction]

    protected def list: Opts[ActionGraph => Unit] = Opts {
      actionGraph => actionGraph.linearized.foreach(println)
    }

    protected def resume: Opts[(ActionGraph, String, SparkSession, StageAction ~> SparkAction) => Unit] = Opts {
      (actionGraph, stage, session, compiler) => actionGraph.resume(stage, session, _ => compiler)
    }

    protected def run: Opts[(ActionGraph, List[String], SparkSession, StageAction ~> SparkAction) => Unit] = Opts {
      (actionGraph, stages, session, compiler) =>
        stages.foreach {
          stageName => actionGraph.runAction(stageName, session, compiler)
        }
    }

    protected def runAll: Opts[(ActionGraph, SparkSession, StageAction ~> SparkAction) => Unit] = Opts {
      (actionGraph, session, compiler) => actionGraph.runAll(session, _ => compiler)
    }

    protected def listCommand: Opts[Unit] =
      Opts.subcommand(
        name = "list",
        help = "List all stages.") {
        list <*> actionGraph
      }

    protected def resumeCommand: Opts[Unit] = Opts.subcommand(
      name = "resume",
      help = "Resume computation from specified stage.") {
      val validated = (actionGraph, Opts.argument[String]("stage"), session, compiler).tupled.mapValidated {
        case args@(actionGraph, stage, _, _) =>
          if (actionGraph.actions.contains(stage)) {
            Validated.valid(args)
          }
          else {
            Validated.invalidNel(s"Stage $stage not found!")
          }
      }
      resume.map[((ActionGraph, String, SparkSession, StageAction ~> SparkAction)) => Unit] {
        fn => a => fn.tupled(a)
      } <*> validated
    }

    protected def runCommand: Opts[Unit] = Opts.subcommand(
      name = "run",
      help = "Execute specified stage(s).") {
      val validated = (actionGraph, Opts.arguments[String]("stage"), session, compiler).tupled.mapValidated {
        case args@(actionGraph, stages, _, _) =>
          stages.filterNot(actionGraph.actions.contains) match {
            case Nil => Validated.valid(args)
            case missing => Validated.invalidNel(s"Stages(s) ${missing.mkString(", ")} not found!")
          }
      }
      run.map[((ActionGraph, NonEmptyList[String], SparkSession, StageAction ~> SparkAction)) => Unit] {
        fn => a => fn(a._1, a._2.toList, a._3, a._4)
      } <*> validated
    }

    protected def runAllCommand: Opts[Unit] = Opts.subcommand(
      name = "runAll",
      help = "Execute all stages.") {
      runAll.map[((ActionGraph, SparkSession, StageAction ~> SparkAction)) => Unit] {
        fn => a => fn.tupled(a)
      } <*> (actionGraph, session, compiler).tupled
    }

    def main: Opts[Unit] = listCommand orElse resumeCommand orElse runCommand orElse runAllCommand
  }

  implicit def enkiMainToOpts(enkiMain: EnkiMain): Opts[Unit] = enkiMain.main
}