package enki
package configuration

//TODO: нужны стабильные идентификаторы стейджей. Нужен дополнительный класс, который будет в себе содержать конфигурацию и символ.
case class SourceConfiguration(schema: Option[String]) {
  private[enki] def resolveTableName(table: Symbol): String = configuration.resolveTableName(schema, table)
}
