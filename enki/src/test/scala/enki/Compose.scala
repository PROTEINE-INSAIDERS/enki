package enki

import cats._
import cats.implicits._
import enki.default.{sparkHandler => _, _}
import freestyle.free._
import freestyle.free.implicits._
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

class Compose extends EnkiTestSuite with enki.default.Database {

  import implicits._

  override def schema: String = "default"

  "compose" in {
    val tableNameMapper: SparkAlg.Op ~> SparkAlg.Op = Lambda[SparkAlg.Op ~> SparkAlg.Op] {
      case SparkAlg.WriteDatasetOp(schemaName, tableName, encoder, strict) => SparkAlg.WriteDatasetOp(schemaName, tableName + "_test", encoder, strict)
      case other => other
    }

    val program = write[(Int, Int)]("yoba") <*> sparkSession.emptyDataset[(Int, Int)].pure[Stage]

    implicit val myCompiler: FSHandler[SparkAlg.Op, StageMonad] = tableNameMapper andThen enki.sparkHandler

    program.interpret[StageMonad](implicitly, interpretIotaCopK[StageOp, StageMonad]).run(Environment(sparkSession))
    sparkSession.sql(s"show tables in $schema").show
  }
}
