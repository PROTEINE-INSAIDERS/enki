package enki

import cats.implicits._
import org.apache.spark.sql.SparkSession

trait SessionModule {

  implicit class SessionExtensions(session: SparkSession) {
    def run[T](stage: Stage[T]): T = {
      stage.foldMap(stageCompiler).apply(session)
    }
  }

}
