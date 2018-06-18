import cats.free.FreeApplicative
import enki.program.Statement
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.reflect.io._

package object enki {
  type Program[A] = FreeApplicative[Statement, A]

  def schemaFromResource(root: Symbol): Symbol => Option[StructType] = {
    val sqlParser = new SparkSqlParser(new SQLConf())
    val rootPath = Path("/" + root.name)
    name => {
      val path = (rootPath / name.name).addExtension("sql")
      Option(getClass.getResourceAsStream(path.path)).map { source =>
        try {
          val script = scala.io.Source.fromInputStream(source).mkString
          sqlParser.parsePlan(script).asInstanceOf[CreateTable].tableDesc.schema
        } finally {
          source.close()
        }
      }
    }
  }
}
