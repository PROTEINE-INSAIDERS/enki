package enki.pm.cli

// Фиксируем вопросы пользователю.
trait Prompt[F[_]] {
  def ask[A](question: Question[A]): F[A]
}
