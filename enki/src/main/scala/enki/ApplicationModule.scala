package enki

import cats._
import cats.implicits._
import cats.data._
import com.monovore.decline._
import org.apache.spark.sql.{SparkSession, _}

import cats.implicits._
import com.monovore.decline._

trait ApplicationModule {

  def defaultMain(
                   actionGraph: ActionGraph,
                   session: SparkSession = SparkSession.builder().getOrCreate()
                 ): Opts[Unit] = {
    val ag = actionGraph
    val s = session
    new EnkiMain {
      override def actionGraph: enki.ActionGraph = ag

      override def compiler: StageAction ~> SparkAction = stageCompiler

      override def session: SparkSession = s
    }.main
  }

  trait EnkiMain {
    protected def actionGraph: Opts[ActionGraph]

    protected def listCommand: Opts[Unit] =
      Opts.subcommand(name = "list", help = "List all stages.") {
        actionGraph.map(_.linearized.foreach(println))
      }

    protected def resumeCommand: Opts[Unit] = Opts.subcommand(
      name = "resume",
      help = "Resume computation from specified stage.") {

      val ttt = (Opts.argument[String]("action"), actionGraph).tupled

      /*
      val aga = Opts.argument[String]("action").product(actionGraph).mapValidated {
        case(action, graph) => ???
      }

      val sss = (Opts.argument[String]("action"), actionGraph) mapN  { (aa, bb) => ??? }
      */
      ???
    }


    /*
    (Opts.argument[String]("action").mapValidated { stage =>
      if (actionGraph.actions.contains(stage)) {
        Validated.valid(stage)
      } else {
        Validated.invalidNel(s"Stage $stage not found!")
      }
    } map { stage =>
      actionGraph.resume(stage, session, _ => compiler)
    })
*/
    protected def run: Opts[Unit] = ??? /* Opts.subcommand(
      name = "run",
      help = "Execute specified stage."
    )(Opts.arguments[String]("stage").mapValidated { stages =>
      stages.filterNot(actionGraph.actions.contains) match {
        case Nil => Validated.valid(stages)
        case missing => Validated.invalidNel(s"Stages(s) ${missing.mkString(", ")} not found!")
      }
    } map { stage =>
      stage.toList.foreach { stageName => actionGraph.runAction(stageName, session, compiler) }
    })*/

    protected def runAll: Opts[Unit] = ??? /* Opts.subcommand(
      name = "runAll",
      help = "Execute all stages."
    )(Opts {
      actionGraph.runAll(session, _ => compiler)
    })
*/
    def main: Opts[Unit] = listCommand orElse resumeCommand orElse run orElse runAll
  }

  implicit def enkiMainToOpts(enkiMain: EnkiMain): Opts[Unit] = enkiMain.main
}