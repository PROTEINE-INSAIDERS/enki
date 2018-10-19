package enki.pm.cli

import cats.effect.Bracket

// системная консоль - ввод вывод + стили.
trait Console[F[_]] {
  def print(str: String): F[Unit]

  def printLn(str: String): F[Unit]

  def readLine: F[String]

  def setStyle(style: Style.Style): F[Unit]

  def withStyle[B, E](style: Style.Style)(f: F[B])(implicit bracket: Bracket[F, E]): F[B] =
    bracket.bracket(setStyle(style))(_ => f)(_ => setStyle(Style.RESET))
}