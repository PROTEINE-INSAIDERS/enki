package enki

import cats._
import cats.implicits._
import enki.default.{defaultStageCompiler => _, _}
import freestyle.free._
import freestyle.free.implicits._

class Compose extends EnkiTestSuite with enki.default.Database {

  import implicits._

  override def schema: String = "default"

  "compose" in {
    val tableNameMapper: StageAlg.Op ~> StageAlg.Op = Lambda[StageAlg.Op ~> StageAlg.Op] {
      case StageAlg.WriteDatasetOp(schemaName, tableName, encoder, strict) => StageAlg.WriteDatasetOp(schemaName, tableName + "_test", encoder, strict)
      case other => other
    }

    val program = write[(Int, Int)]("yoba") <*> sparkSession.emptyDataset[(Int, Int)].pure[Stage]

    implicit val myCompiler: FSHandler[StageAlg.Op, enki.EnkiMonad] = tableNameMapper andThen enki.default.defaultStageCompiler

    program.interpret[EnkiMonad](implicitly, interpretIotaCopK[StageOp, EnkiMonad]).run(Environment(sparkSession))
    sparkSession.sql(s"show tables in $schema").show
  }
}