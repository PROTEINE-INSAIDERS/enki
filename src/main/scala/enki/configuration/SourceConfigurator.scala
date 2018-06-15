package enki
package configuration

// нам нужен не просто конфигуратор, а объект, который способен выполнять все действия с источником.
trait SourceConfigurator {
  protected[enki] def resolveTableName(table: Symbol): String
}
