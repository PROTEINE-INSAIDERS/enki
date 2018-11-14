package enki.pm.cli

import java.nio.file.Path

import cats._
import cats.implicits._
import cats.effect._
import cats.effect.implicits._
import cats.effect.concurrent._
import cats.implicits._
import enki.pm.fs._

trait AnswerRecorder[F[_]] {
  def get(key: String): F[Option[String]]

  def store(key: String, value: String): F[Unit]
}

case class ForgetfulRecorder[F[_] : Applicative]() extends AnswerRecorder[F] {
  override def get(key: String): F[Option[String]] = Option.empty[String].pure[F]

  override def store(key: String, value: String): F[Unit] = ().pure[F]
}

case class CacheRecorder[F[_] : Functor](
                                          answers: Ref[F, Map[String, String]]
                                        ) extends AnswerRecorder[F] {

  override def get(key: String): F[Option[String]] = answers.get fmap (_.get(key))

  override def store(key: String, value: String): F[Unit] = answers.update(_ + (key -> value))
}


