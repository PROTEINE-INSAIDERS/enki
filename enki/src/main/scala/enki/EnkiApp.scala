package enki

import cats.data._
import com.monovore.decline._
import org.apache.spark.sql._

case class EnkiApp(
                    name: String,
                    header: String,
                    actionGraph: ActionGraph,
                    session: SparkSession = SparkSession.builder().getOrCreate()) extends CommandApp(
  name = name,
  header = header,
  main = {
    val list = Opts.subcommand(
      name = "list",
      help = "List all stages."
    )(Opts {
      actionGraph.linearized.foreach(println)
    })

    val run = Opts.subcommand(
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

    val runAll = Opts.subcommand(
      name = "runAll",
      help = "Execute all stages."
    )(Opts {
      actionGraph.runAll(session, _ => stageCompiler)
    })

    val resume = Opts.subcommand(
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

    list orElse resume orElse run orElse runAll
  }
)