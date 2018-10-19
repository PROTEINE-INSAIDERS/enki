package enki.pm.cli

import cats.effect._
import cats.implicits._
import enki.pm.cli.Style.Style

class SystemConsole[F[_]](implicit f: LiftIO[F]) extends Console[F] {
  private def withColor(a: IO[Unit], color: Option[Style.Style]): IO[Unit] = color match {
    case Some(c) => IO(scala.Console.out.print(c.ansi)) *> a *> IO(scala.Console.out.print(scala.Console.RESET))
    case None => a
  }

  override def print(string: String): F[Unit] = f.liftIO {
    IO(scala.Console.out.print(string))
  }

  override def printLn(string: String): F[Unit] = f.liftIO {
    IO(scala.Console.out.println(string))
  }

  override def readLine: F[String] = f.liftIO {
    IO(scala.Console.in.readLine())
  }

  override def setStyle(style: Style): F[Unit] = f.liftIO {
    IO(scala.Console.out.print(style.ansi))
  }
}
