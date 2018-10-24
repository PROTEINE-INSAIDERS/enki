package enki.pm.cli

// Форматировщик - фиксация основных стилей.
//TODO: Убрать, стили логгера перенести в логгер, стили Prompt-а в prompt.
trait Formatter[F[_]] {
  def withDebug[A](f: F[A]): F[A]

  def withError[A](f: F[A]): F[A]

  def withInfo[A](f: F[A]): F[A]

  def withInput[A](f: F[A]): F[A]

  def withInvalidInput[A](f: F[A]): F[A]

  def withTrace[A](f: F[A]): F[A]

  def withWarn[A](f: F[A]): F[A]
}
