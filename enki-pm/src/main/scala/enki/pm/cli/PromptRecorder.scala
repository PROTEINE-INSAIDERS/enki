package enki.pm.cli

import cats._
import cats.implicits._

trait PromptRecorder[F[_]] {
  def get(key: String): F[Option[String]]

  def store(key: String, value: String): F[Unit]
}

case class NullRecorder[F[_] : Applicative]() extends PromptRecorder[F] {
  override def get(key: String): F[Option[String]] = Option.empty[String].pure[F]

  override def store(key: String, value: String): F[Unit] = ().pure[F]
}