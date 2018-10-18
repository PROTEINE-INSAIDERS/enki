package enki.pm.cli

import cats._
import cats.implicits._

class ConsoleWithTagsHandler[F[_]: Applicative](implicit console: Console[F]) extends ConsoleWithTags.Handler[F] {
  private val questionColor = Color.CYAN
  private val questionTag = "QUESTION"

  private def tag(s: String, color: Color.Color) =
    console.print("[") *> console.print(s, Some(color)) *> console.print("]")

  override def question(s: String): FS[Unit] = {
    tag(questionTag, questionColor) *> console.print(s" $s")
  }
}
