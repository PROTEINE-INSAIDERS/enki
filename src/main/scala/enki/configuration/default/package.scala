package enki
package configuration

package object default {
  private val defaultKeySuffix = "default"

  private def defaultKey(key: String): String = s"${configuration.enkiKey}.$key.${defaultKeySuffix}"

  implicit val Right(sourceConfiguration) = pureconfig.loadConfig[SourceConfiguration](defaultKey("sources"))
}
