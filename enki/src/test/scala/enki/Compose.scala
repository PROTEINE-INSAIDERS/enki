package enki

import cats._
import cats.implicits._
import enki.default.{sparkHandler => _, _}
import freestyle.free._
import freestyle.free.implicits._

class Compose extends EnkiTestSuite with enki.default.Database {

  import implicits._

  override def schema: String = "default"

  "compose" in {
    val tableNameMapper: SparkAlg.Op ~> SparkAlg.Op = Lambda[SparkAlg.Op ~> SparkAlg.Op] {
      case SparkAlg.WriteDatasetOp(schemaName, tableName, encoder, strict) => SparkAlg.WriteDatasetOp(schemaName, tableName + "_test", encoder, strict)
      case other => other
    }

    val program = write[(Int, Int)]("yoba") <*> sparkSession.emptyDataset[(Int, Int)].pure[Stage]

    implicit val myCompiler: FSHandler[SparkAlg.Op, enki.EnkiMonad] = tableNameMapper andThen enki.sparkHandler

    program.interpret[EnkiMonad](implicitly, interpretIotaCopK[StageOp, EnkiMonad]).run(Environment(sparkSession))
    sparkSession.sql(s"show tables in $schema").show
  }
}
