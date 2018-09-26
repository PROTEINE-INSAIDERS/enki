
import com.monovore.decline._
import enki._
import enki.testsuite.EnkiSuite
import freestyle.free._
import org.apache.spark.sql._

@module trait StagesWithArguments {
  val stage: Stage
  val args: Args
}

object Main extends CommandApp(
  name = "tutorial",
  header = "Enki tutorial",
  main = new TutorialMain[StagesWithArguments.Op, TutorialMain.tutorProgram.ProgramM.Op])

object TutorialMain {
  val tutorProgram = new ProgramWrapper[StagesWithArguments.Op]
}

class TutorialMain[StageAlg[_], ProgramAlg[_]](
                                                implicit val args: enki.Args[StageAlg],
                                                implicit val stage: enki.Stage[StageAlg],
                                                implicit val program: enki.Program1[StageAlg, ProgramAlg]
                                              )
  extends EnkiMain
    // with UserDatabase
    with SourceDatabase[StageAlg, ProgramAlg]
    with EnkiSuite {

  override def actionGraph: enki.ActionGraph = buildActionGraph("root", ???)

  override def session: Opts[SparkSession] = Opts {
    SparkSession.builder().master(s"local").getOrCreate()
  }

  /*
    override def compiler: Opts[~>[enki.StageAction, SparkAction]] = Opts {
      stageCompiler
    }

    override def resume(action: String): Opts[SparkSession => StageAction ~> SparkAction => Unit] = {
      super.resume(action).map { f =>
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
    */
}