import enki._
import org.apache.spark.sql._
import org.apache.log4j._

object Main extends EnkiApp(
  name = "tutorial",
  header = "Enki tutorial",
  actionGraph = buildActionGraph("root", UserDatabase.program),
  session = SparkSession.builder().master(s"local").getOrCreate()) {
  {
    SourceDatabase.createDemoTables(session)
    UserDatabase.createDatabase(session)
  }
}