package enki.pm.cli

import cats.effect._
import cats.implicits._

class SystemConsole[F[_]](implicit f: LiftIO[F]) extends Console.Handler[F] {
  private def withColor(a: IO[Unit], color: Option[Color.Color]): IO[Unit] = color match {
    case Some(c) => IO(scala.Console.out.print(c.ansi)) *> a *> IO(scala.Console.out.print(scala.Console.RESET))
    case None => a
  }

  override def print(string: String, color: Option[Color.Color]): FS[Unit] = f.liftIO {
    withColor(IO(scala.Console.out.print(string)), color)
  }


  override def printLn(string: String, color: Option[Color.Color]): FS[Unit] = f.liftIO {
    withColor(IO(scala.Console.out.println(string)), color)
  }

  override def readLine(): FS[String] = f.liftIO {
    IO(scala.Console.in.readLine())
  }
}
