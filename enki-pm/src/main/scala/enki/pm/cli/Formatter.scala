package enki.pm.cli

// Форматировщик - фиксация основных стилей.
trait Formatter[F[_]] {
  def withInvalidInput[A](f: F[A]): F[A]

  def withQuestion[A](f: F[A]): F[A]
}
