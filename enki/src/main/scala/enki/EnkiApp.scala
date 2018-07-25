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
    Opts.subcommand(
      name = "list",
      help = "List all actions."
    )(Opts {
      actionGraph.linearized.foreach(println)
    }) orElse
      Opts.subcommand(
        name = "run",
        help = "Execute specified actions."
      )(Opts.arguments[String]("action").mapValidated { actions =>
        actions.filterNot(actionGraph.actions.contains) match {
          case Nil => Validated.valid(actions)
          case missing => Validated.invalidNel(s"Action(s) ${missing.mkString(", ")} not found!")
        }
      } map { actions =>
        actions.toList.foreach(actionGraph.runAction(_)(session))
      }) orElse
      Opts.subcommand(
        name = "runAll",
        help = "Execute all actions."
      )(Opts {
        actionGraph.runAll(session)
      })
  }
)