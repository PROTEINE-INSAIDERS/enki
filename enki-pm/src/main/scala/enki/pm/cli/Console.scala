package enki.pm.cli

import freestyle.tagless._

import scala.io.AnsiColor

object Color extends Enumeration {
  type Color = ColorValue
  case class ColorValue(ansi: String) extends Val(nextId)

  val CYAN = ColorValue(AnsiColor.CYAN)
}

@tagless(true) trait Console {
  def print(str: String, color: Option[Color.Color] = None): FS[Unit]
  def printLn(str: String, color: Option[Color.Color] = None): FS[Unit]
  def readLine(): FS[String]
}
