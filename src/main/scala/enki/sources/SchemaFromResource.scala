package enki.sources

import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import scala.reflect.io._

trait SchemaFromResource extends SchemaProvider {
  private val sqlParser = new SparkSqlParser(new SQLConf())

  def root: Path

  override def getSchema(name: Symbol): Option[StructType] = {
    val path = root / Path(name.name) addExtension "sql"
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
