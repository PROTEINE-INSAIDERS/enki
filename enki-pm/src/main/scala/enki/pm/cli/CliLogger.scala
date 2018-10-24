package enki.pm.cli

import cats._
import cats.effect._
import cats.implicits._
import io.chrisdavenport.log4cats._

class CliLogger[F[_] : Applicative, E](implicit c: Console[F], b: Bracket[F, E]) extends Logger[F] with CliTag {
  private val debudTag = Tag("DEBUG", None)
  private val errorTag = Tag("ERROR", Some(Style.RED))
  private val infoTag = Tag("INFO", Some(Style.GREEN))
  private val traceTag = Tag("TRACE", None)
  private val warnTag = Tag("TRACE", Some(Style.YELLOW))

  private def formatMessage(t: Throwable, message: String): F[Unit] =
    c.printLn(message) *> c.withStyle(Style.BOLD)(c.printLn(" CAUSED BY ")) *> c.printLn(t.toString)

  override def error(message: => String): F[Unit] = withTag(errorTag)(c.printLn(message))

  override def warn(message: => String): F[Unit] = withTag(warnTag)(c.printLn(message))

  override def info(message: => String): F[Unit] = withTag(infoTag)(c.printLn(message))

  override def debug(message: => String): F[Unit] = withTag(debudTag)(c.printLn(message))

  override def trace(message: => String): F[Unit] = withTag(traceTag)(c.printLn(message))

  override def error(t: Throwable)(message: => String): F[Unit] = withTag(errorTag)(formatMessage(t, message))

  override def warn(t: Throwable)(message: => String): F[Unit] = withTag(warnTag)(formatMessage(t, message))

  override def info(t: Throwable)(message: => String): F[Unit] = withTag(infoTag)(formatMessage(t, message))

  override def debug(t: Throwable)(message: => String): F[Unit] = withTag(debudTag)(formatMessage(t, message))

  override def trace(t: Throwable)(message: => String): F[Unit] = withTag(traceTag)(formatMessage(t, message))
}
