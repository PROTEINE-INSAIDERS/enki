package enki.pm.cli

import cats._
import cats.implicits._

class BootstrapPrompt[F[_] : Applicative](implicit formatter: Formatter[F], console: Console[F]) extends Prompt[F] {
  private def autoAnswer[A](question: Question[A]): Option[A] = question match {
    case WhereDoYouWantToGoToday() => Some("Microsoft™")
  }

  override def ask[A](question: Question[A]): F[A] = {
    formatter.withQuestion(console.print(question.questionStr)) *> console.print(" ") *> {
      autoAnswer(question) match {
        case Some(answer) => console.printLn(answer.toString) *> answer.pure[F]
        case None => ??? // TODO: добавить tpolecat/atto, распарсить ответ в A,
        // если ошибка, спросить снова, иначе вернуть ответ (потребует замену Applicative на Monad).
      }
    }
  }
}

