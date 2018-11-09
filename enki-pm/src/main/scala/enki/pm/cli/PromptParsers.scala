package enki.pm.cli

import java.nio.file.{Path, Paths}

import atto.Atto._
import atto._
import cats.implicits._

import scala.util.Try

class PromptParsers extends Prompt[Parser] {
  private def bool: Parser[Boolean] = orElse(oneOf("Yy") *> true.pure[Parser], oneOf("Nn") *> false.pure[Parser])

  override def createNewProject(path: Path): Parser[Boolean] = bool

  override def sqlRoot: Parser[String] = takeText

  override def projectName: Parser[String] = takeText

  override def projectDir: Parser[Path] = takeText >>= { str =>
    Try {
      Paths.get(str).pure[Parser]
    }.recover {
      case e: Throwable => err[Path](e.getLocalizedMessage)
    }.get
  }
}
