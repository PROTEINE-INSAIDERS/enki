package enki.pm.cli

trait AnswerRecorder[F[_]] {
  def get(key: String): F[Option[String]]
  def store(key: String, value: String): F[Unit]
}
