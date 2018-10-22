package enki.pm.cli

import scala.io.AnsiColor

object Style extends Enumeration {
  type Style = StyleValue
  case class StyleValue(ansi: String) extends Val(nextId)

  val CYAN = StyleValue(AnsiColor.CYAN)

  val MAGENTA = StyleValue(AnsiColor.MAGENTA)

  val RESET = StyleValue(AnsiColor.RESET)
}