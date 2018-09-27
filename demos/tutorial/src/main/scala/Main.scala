
import com.monovore.decline._
import enki.default._
import enki.testsuite.EnkiSuite
import freestyle.free._
import org.apache.spark.sql._


class Aaa (implicit val aaa: enki.Args[StageOp])

object Test {
  def main(args: Array[String]): Unit = {
    val a = new Aaa()
    println(a.aaa)
  }
}

object Main extends CommandApp(
  name = "tutorial",
  header = "Enki tutorial",
  main = {
    new TutorialMain()(
      implicitly,
      implicitly,
      implicitly)
  })

class TutorialMain(
                    implicit val args: enki.Args[StageOp],
                    implicit val stageAlg: enki.Stage[StageOp], //TODO: это убрать бы надо..
                    implicit val p: enki.Program1[StageOp, ProgramOp]
                  )
  extends EnkiMain
    // with UserDatabase
    with SourceDatabase
    with UserDatabase
    with EnkiSuite {

  override def actionGraph: enki.ActionGraph = buildActionGraph("root", createReports)

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