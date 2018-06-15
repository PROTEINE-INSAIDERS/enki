package enki
package configuration

package object default {
  private[enki] def resolveTableName(schema: Option[String], table: Symbol): String = {
    schema.map(schemaName => s"$schemaName.${table.name}").getOrElse(table.name)
  }

  implicit val sourceConfigurator: SourceConfigurator = new SourceConfigurator {
    override protected[enki] def resolveTableName(table: Symbol): String = default.resolveTableName(None, table)
  }
}
