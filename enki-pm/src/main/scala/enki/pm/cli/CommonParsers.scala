package enki.pm.cli

import atto.Atto._
import atto._
import cats.implicits._

trait CommonParsers {
  protected def bool: Parser[Boolean] = orElse(oneOf("Yy") *> true.pure[Parser], oneOf("Nn") *> false.pure[Parser])
}
