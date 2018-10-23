package enki.pm.cli

import scala.io.AnsiColor

object Style extends Enumeration {
  type Style = StyleValue
  case class StyleValue(ansi: String) extends Val(nextId)

  val BLACK = StyleValue(AnsiColor.BLACK)

  val RED = StyleValue(AnsiColor.RED)

  val GREEN = StyleValue(AnsiColor.GREEN)

  val YELLOW = StyleValue(AnsiColor.YELLOW)

  val BLUE = StyleValue(AnsiColor.BLUE)

  val MAGENTA = StyleValue(AnsiColor.MAGENTA)

  val CYAN = StyleValue(AnsiColor.CYAN)

  val WHITE = StyleValue(AnsiColor.WHITE)

  val RESET = StyleValue(AnsiColor.RESET)

  val BOLD = StyleValue(AnsiColor.BOLD)
}