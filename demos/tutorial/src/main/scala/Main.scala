import com.monovore.decline.CommandApp
import enki._
import enki.tests.DatabaseState
import org.apache.spark.sql._

object Main extends CommandApp(
  name = "tutorial",
  header = "Enki tutorial",
  main = defaultMain(
    buildActionGraph("root", UserDatabase.program),
    {
      val session = SparkSession.builder().master(s"local").getOrCreate()
      //      SourceDatabase.createDemoTables(session)
      //      UserDatabase.createDatabase(session)
      new DatabaseState {
        override protected def graph: enki.ActionGraph = buildActionGraph("root", UserDatabase.program)
      }.emulate(session)

      session
    }
  ))