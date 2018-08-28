import cats._
import com.monovore.decline._
import enki._
import enki.testsuite.EnkiSuite
import org.apache.spark.sql._

object Main extends CommandApp(
  name = "tutorial",
  header = "Enki tutorial",
  main = new TutorialMain())

class TutorialMain extends EnkiMain with UserDatabase with SourceDatabase with EnkiSuite {
  override def actionGraph: enki.ActionGraph = buildActionGraph("root", program)

  override def session: Opts[SparkSession] = Opts {
    SparkSession.builder().master(s"local").getOrCreate()
  }

  override def compiler: Opts[~>[enki.StageAction, SparkAction]] = Opts {
    stageCompiler
  }

  override def resume1(action: String): Opts[SparkSession => StageAction ~> SparkAction => Unit] = {
    super.resume1(action).map { f =>
      session =>
        compiler =>
          createEmptySources(actionGraph, session) // intercepting enki commands to create sample tables
          f(session)(compiler)
    }
  }

  override def run(action: String): Opts[SparkSession => StageAction ~> SparkAction => Unit] = {
    super.run(action).map { f =>
      session =>
        compiler =>
          createEmptySources(actionGraph, session) // intercepting enki commands to create sample tables
          f(session)(compiler)
    }
  }

  override def runAll: Opts[SparkSession => StageAction ~> SparkAction => Unit] = {
    super.runAll.map { f =>
      session =>
        compiler =>
          createEmptySources(actionGraph, session) // intercepting enki commands to create sample tables
          f(session)(compiler)
    }
  }
}