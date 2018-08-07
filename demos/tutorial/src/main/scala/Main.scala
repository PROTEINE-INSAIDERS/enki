import com.monovore.decline.CommandApp
import enki._
import enki.tests.EnkiSuite
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
      new EnkiSuite {}.createEmptySources(buildActionGraph("root", UserDatabase.program),session)

      session
    }
  ))