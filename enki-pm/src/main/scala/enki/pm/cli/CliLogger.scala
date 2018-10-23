package enki.pm.cli

import cats._
import cats.effect._
import cats.implicits._
import io.chrisdavenport.log4cats._

class CliLogger[F[_] : Applicative, E](implicit f: Formatter[F], c: Console[F], b: Bracket[F, E]) extends Logger[F] {
  private def formatMessage(t: Throwable, message: String): F[Unit] =
    c.printLn(message) *> c.withStyle(Style.BOLD)(c.printLn(" CAUSED BY ")) *> c.printLn(t.toString)

  override def error(message: => String): F[Unit] = f.withError(c.printLn(message))

  override def warn(message: => String): F[Unit] = f.withError(c.printLn(message))

  override def info(message: => String): F[Unit] = f.withInfo(c.printLn(message))

  override def debug(message: => String): F[Unit] = f.withDebug(c.printLn(message))

  override def trace(message: => String): F[Unit] = f.withTrace(c.printLn(message))

  override def error(t: Throwable)(message: => String): F[Unit] = f.withError(formatMessage(t, message))

  override def warn(t: Throwable)(message: => String): F[Unit] = f.withWarn(formatMessage(t, message))

  override def info(t: Throwable)(message: => String): F[Unit] = f.withInfo(formatMessage(t, message))

  override def debug(t: Throwable)(message: => String): F[Unit] = f.withDebug(formatMessage(t, message))

  override def trace(t: Throwable)(message: => String): F[Unit] = f.withTrace(formatMessage(t, message))
}
