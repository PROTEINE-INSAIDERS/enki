package enki.sources

import org.apache.spark.sql.types.StructType

trait SchemaProvider {
  def getSchema(name: Symbol): Option[StructType]
}
