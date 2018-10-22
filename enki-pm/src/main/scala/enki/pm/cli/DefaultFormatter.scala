package enki.pm.cli

import cats._
import cats.implicits._
import cats.effect._

class DefaultFormatter[F[_]: Applicative, E](implicit console: Console[F], bracket: Bracket[F, E]) extends Formatter[F] {
  private case class Tag(tag: String, style: Style.Style)

  private val invalidInputTag = Tag("INVALID", Style.MAGENTA)

  private val questionTag = Tag("QUESTION", Style.CYAN)

  private def withTag[A](tag: Tag)(f: F[A]): F[A] =
    console.print("[") *> console.withStyle(tag.style)(console.print(tag.tag)) *> console.print("] ") *> f

  override def withInvalidInput[A](f: F[A]): F[A] = withTag(invalidInputTag)(f)

  override def withQuestion[A](f: F[A]): F[A] = withTag(questionTag)(f)
}
