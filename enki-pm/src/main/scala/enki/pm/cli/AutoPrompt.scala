package enki.pm.cli

import cats._
import cats.implicits._

class AutoPrompt[F[_] : Applicative](implicit console: Console[F]) extends Prompt.Handler[F] {
  private def autoAnswer[A](question: Question[A]): A = question match {
    case WhereDoYouWantToGoToday() => "Microsoft™"
  }

  override def ask[A](question: Question[A]): FS[A] = {
    val answer = autoAnswer(question)
    console.print(question.questionStr) *> console.print(" ") *> answer.pure[FS]
  }
}

