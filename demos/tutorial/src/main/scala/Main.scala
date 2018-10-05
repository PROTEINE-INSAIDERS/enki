
import com.monovore.decline._
import enki.default._
import enki.testsuite.EnkiSuite
import org.apache.spark.sql._

object Main extends CommandApp(
  name = "tutorial",
  header = "Enki tutorial",
  main = new TutorialMain())

class TutorialMain
  extends EnkiMain
    // with UserDatabase
    with SourceDatabase
    with UserDatabase
    with EnkiSuite {

  override def actionGraph: ActionGraph = buildActionGraph("root", createReports)

  override def session: Opts[SparkSession] = Opts {
    SparkSession.builder().master(s"local").getOrCreate()
  }

  override def resume(action: String): Opts[SparkSession => StageHandler => Unit] = {
    super.resume(action).map { f =>
      session =>
        compiler =>
          createEmptySources(actionGraph, session) // intercepting enki commands to create sample tables
          f(session)(compiler)
    }
  }

  override def run(action: String): Opts[SparkSession => StageHandler => Unit] = {
    super.run(action).map { f =>
      session =>
        compiler =>
          createEmptySources(actionGraph, session) // intercepting enki commands to create sample tables
          f(session)(compiler)
    }
  }

  override def runAll: Opts[SparkSession => StageHandler => Unit] = {
    super.runAll.map { f =>
      session =>
        compiler =>
          createEmptySources(actionGraph, session) // intercepting enki commands to create sample tables
          f(session)(compiler)
    }
  }
}