package enki.pm.cli

import java.nio.file.{Path, Paths}

import atto.Atto._
import atto._
import cats.implicits._

import scala.util.Try

class PromptParsers extends Prompt[Parser] {
  override def sqlRoot: Parser[String] = takeText

  override def projectName: Parser[String] = takeText
}
