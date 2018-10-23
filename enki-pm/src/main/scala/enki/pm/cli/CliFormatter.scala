package enki.pm.cli

import cats._
import cats.effect._
import cats.implicits._

class CliFormatter[F[_] : Applicative, E](implicit console: Console[F], bracket: Bracket[F, E]) extends Formatter[F] {

  private case class Tag(tag: String, style: Option[Style.Style])

  private val debudTag = Tag("DEBUG", None)
  private val errorTag = Tag("ERROR", Some(Style.RED))
  private val infoTag = Tag("INFO", Some(Style.GREEN))
  private val inputTag = Tag("INPUT", Some(Style.CYAN))
  private val invalidInputTag = Tag("INVALID", Some(Style.MAGENTA))
  private val traceTag = Tag("TRACE", None)
  private val warnTag = Tag("TRACE", Some(Style.YELLOW))

  private def withTag[A](tag: Tag)(f: F[A]): F[A] =
    console.print("[") *>
      tag.style.map(console.withStyle(_)(console.print(tag.tag))).getOrElse(console.print(tag.tag)) *>
      console.print("] ") *>
      f

  override def withDebug[A](f: F[A]): F[A] = withTag(debudTag)(f)

  override def withError[A](f: F[A]): F[A] = withTag(errorTag)(f)

  override def withInfo[A](f: F[A]): F[A] = withTag(infoTag)(f)

  override def withInput[A](f: F[A]): F[A] = withTag(inputTag)(f)

  override def withInvalidInput[A](f: F[A]): F[A] = withTag(invalidInputTag)(f)

  override def withTrace[A](f: F[A]): F[A] = withTag(traceTag)(f)

  override def withWarn[A](f: F[A]): F[A] = withTag(warnTag)(f)
}
