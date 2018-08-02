import com.monovore.decline.CommandApp
import enki._
import org.apache.spark.sql._

object Main extends CommandApp(
  name = "tutorial",
  header = "Enki tutorial",
  main = defaultMain(
    buildActionGraph("root", UserDatabase.program),
    {
      val session = SparkSession.builder().master(s"local").getOrCreate()
      SourceDatabase.createDemoTables(session)
      UserDatabase.createDatabase(session)
      session
    }
  ))