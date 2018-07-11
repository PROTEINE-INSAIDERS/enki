package enki

import com.monovore.decline._
import org.apache.spark.sql.SparkSession

class EnkiApp(name: String, header: String, actionGraph: ActionGraph) extends CommandApp(
  name = name,
  header = header,
  main = {
    val actionOpt = Opts
      .option[String]("action", help = "Action to execute.")
      .validate("Action not found")(actionGraph.actions.contains)

    actionOpt map (actionGraph.runAction(_)(SparkSession.builder().getOrCreate()))
  }
)
