package enki

import cats._
import cats.implicits._
import freestyle.free._
import freestyle.free.implicits._
import enki.default._
import enki.default.implicits.{stageOnlyCompiler => _, _}

class Compose extends EnkiTestSuite with enki.default.Database {
  import implicits._

  override def schema: String = "default"

  "compose" in {
    val tableNameMapper: StageAlg.Op ~> StageAlg.Op = Lambda[StageAlg.Op ~> StageAlg.Op] {
      case StageAlg.WriteDatasetOp(schemaName, tableName, encoder, strict) => StageAlg.WriteDatasetOp(schemaName, tableName + "_test", encoder, strict)
      case other => other
    }

    val program = write[(Int, Int)]("yoba") <*> sparkSession.emptyDataset[(Int, Int)].pure[Stage]

    implicit val myCompiler: FSHandler[StageAlg.Op, EnkiMonad]  = tableNameMapper andThen enki.default.implicits.stageOnlyCompiler
    //    implicit val finalCompiler: StageCompiler = interpretIotaCopK

    program.interpret[EnkiMonad](implicitly, interpretIotaCopK).run(Environment(sparkSession))
    sparkSession.sql(s"show tables in $schema").show
  }
}
