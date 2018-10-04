package enki

import cats.implicits._
import freestyle.free.FreeS
import org.apache.spark.sql.SparkSession

trait SessionModule {

  implicit class SessionExtensions(session: SparkSession) {
    /*
    //TODO: пока мне не очень часто требовалось запускать код на дефалтном компиляторе, возможно в этих методах нет особого смысла.
    def run[T](stage: FreeS.Par[Stage.Op, T]): T = {
      ???
      // stage.foldMap(stageCompiler).apply(Environment(session, Map.empty))
    }

    def run[T](program: Program[FreeS.Par[Stage.Op, T]]): Unit = {
      ???
      // val actionGraph = buildActionGraph("root", program)
      // actionGraph.runAll(stageCompiler, Environment(session, Map.empty))
    }
    */
  }
}
