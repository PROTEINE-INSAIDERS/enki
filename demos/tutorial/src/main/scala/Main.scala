import cats._
import com.monovore.decline._
import enki._
import enki.tests.EnkiSuite
import org.apache.spark.sql._

object Main extends CommandApp(
  name = "tutorial",
  header = "Enki tutorial",
  main = new TutorialMain())

class TutorialMain extends EnkiMain with UserDatabase with SourceDatabase with EnkiSuite {
  override def actionGraph: Opts[enki.ActionGraph] = {
    Opts.option[String](
      long = "userOption",
      help = "You can add user-defined options to enki commands and parameters."
    ).withDefault("<none>") map { userOption =>
      println(s"userOption is $userOption")
      buildActionGraph("root", program)
    }
  }

  override def session: Opts[SparkSession] = Opts { SparkSession.builder().master(s"local").getOrCreate() }

  override def compiler: Opts[~>[enki.StageAction, SparkAction]] = Opts { stageCompiler }

  override def resume: Opts[(ActionGraph, String, SparkSession, StageAction ~> SparkAction) => Unit] = {
    super.resume.map { f => (graph, stage, session, compiler) =>
      createEmptySources(graph, session) // intercepting enki commands to create sample tables
      f(graph, stage, session, compiler) }
  }

  override def run: Opts[(ActionGraph, List[String], SparkSession, StageAction ~> SparkAction) => Unit] = {
    super.run.map { f => (graph, stages, session, compiler) =>
      createEmptySources(graph, session) // intercepting enki commands to create sample tables
      f(graph, stages, session, compiler) }
  }

  override def runAll: Opts[(ActionGraph, SparkSession, StageAction ~> SparkAction) => Unit] = {
    super.runAll.map { f => (graph, session, compiler) =>
      createEmptySources(graph, session) // intercepting enki commands to create sample tables
      f(graph, session, compiler) }
  }
}